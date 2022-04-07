// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/encrypt"
	berrors "github.com/tikv/migration/br/pkg/errors"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/summary"
	"go.uber.org/zap"
)

const (
	// LockFile represents file name
	LockFile = "backup.lock"
	// MetaFile represents file name
	MetaFile = "backupmeta"
	// MetaJSONFile represents backup meta json file name
	MetaJSONFile = "backupmeta.json"
	// MaxBatchSize represents the internal channel buffer size of MetaWriter and MetaReader.
	MaxBatchSize = 1024

	// MetaFileSize represents the limit size of one MetaFile
	MetaFileSize = 128 * units.MiB

	// CrypterIvLen represents the length of iv of crypter method
	CrypterIvLen = 16
)

const (
	// MetaV1 represents the old version of backupmeta.
	// because the old version doesn't have version field, so set it to 0 for compatibility.
	MetaV1 = iota
	// MetaV2 represents the new version of backupmeta.
	MetaV2
)

func Encrypt(content []byte, cipher *backuppb.CipherInfo) (encryptedContent, iv []byte, err error) {
	switch cipher.CipherType {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return content, iv, nil
	case encryptionpb.EncryptionMethod_AES128_CTR,
		encryptionpb.EncryptionMethod_AES192_CTR,
		encryptionpb.EncryptionMethod_AES256_CTR:
		// generate random iv for aes crypter
		iv = make([]byte, CrypterIvLen)
		_, err = rand.Read(iv)
		if err != nil {
			return content, iv, errors.Trace(err)
		}
		encryptedContent, err = encrypt.AESEncryptWithCTR(content, cipher.CipherKey, iv)
		return
	default:
		return content, iv, errors.Annotate(berrors.ErrInvalidArgument, "cipher type invalid")
	}
}

func Decrypt(content []byte, cipher *backuppb.CipherInfo, iv []byte) ([]byte, error) {
	switch cipher.CipherType {
	case encryptionpb.EncryptionMethod_PLAINTEXT:
		return content, nil
	case encryptionpb.EncryptionMethod_AES128_CTR,
		encryptionpb.EncryptionMethod_AES192_CTR,
		encryptionpb.EncryptionMethod_AES256_CTR:
		return encrypt.AESDecryptWithCTR(content, cipher.CipherKey, iv)
	default:
		return content, errors.Annotate(berrors.ErrInvalidArgument, "cipher type invalid")
	}
}

func walkLeafMetaFile(
	ctx context.Context,
	storage storage.ExternalStorage,
	file *backuppb.MetaFile,
	cipher *backuppb.CipherInfo,
	output func(*backuppb.MetaFile)) error {
	if file == nil {
		return nil
	}
	if len(file.MetaFiles) == 0 {
		output(file)
		return nil
	}
	for _, node := range file.MetaFiles {
		content, err := storage.ReadFile(ctx, node.Name)
		if err != nil {
			return errors.Trace(err)
		}

		decryptContent, err := Decrypt(content, cipher, node.CipherIv)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(decryptContent)
		if !bytes.Equal(node.Sha256, checksum[:]) {
			return errors.Annotatef(berrors.ErrInvalidMetaFile,
				"checksum mismatch expect %x, got %x", node.Sha256, checksum[:])
		}

		child := &backuppb.MetaFile{}
		if err = proto.Unmarshal(decryptContent, child); err != nil {
			return errors.Trace(err)
		}
		if err = walkLeafMetaFile(ctx, storage, child, cipher, output); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// MetaReader wraps a reader to read both old and new version of backupmeta.
type MetaReader struct {
	storage    storage.ExternalStorage
	backupMeta *backuppb.BackupMeta
	cipher     *backuppb.CipherInfo
}

// NewMetaReader creates MetaReader.
func NewMetaReader(
	backpMeta *backuppb.BackupMeta,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo) *MetaReader {
	return &MetaReader{
		storage:    storage,
		backupMeta: backpMeta,
		cipher:     cipher,
	}
}

// ArchiveSize return the size of Archive data
func (reader *MetaReader) ArchiveSize(ctx context.Context, files []*backuppb.File) uint64 {
	total := uint64(0)
	for _, file := range files {
		total += file.Size_
	}
	return total
}

// AppendOp represents the operation type of meta.
type AppendOp int

const (
	// AppendMetaFile represents the MetaFile type.
	AppendMetaFile AppendOp = 0
	// AppendDataFile represents the DataFile type.
	// it records the file meta from tikv.
	AppendDataFile AppendOp = 1
)

func (op AppendOp) name() string {
	var name string
	switch op {
	case AppendMetaFile:
		name = "metafile"
	case AppendDataFile:
		name = "datafile"
	default:
		log.Panic("unsupport op type", zap.Any("op", op))
	}
	return name
}

// appends item to MetaFile
func (op AppendOp) appendFile(a *backuppb.MetaFile, b interface{}) (int, int) {
	size := 0
	itemCount := 0
	switch op {
	case AppendMetaFile:
		a.MetaFiles = append(a.MetaFiles, b.(*backuppb.File))
		size += int(b.(*backuppb.File).Size_)
		itemCount++
	case AppendDataFile:
		// receive a batch of file because we need write and default sst are adjacent.
		files := b.([]*backuppb.File)
		a.DataFiles = append(a.DataFiles, files...)
		for _, f := range files {
			itemCount++
			size += int(f.Size_)
		}
	}

	return size, itemCount
}

type sizedMetaFile struct {
	// A stack like array, we always append to the last node.
	root      *backuppb.MetaFile
	size      int
	itemNum   int
	sizeLimit int
}

// NewSizedMetaFile represents the sizedMetaFile.
func NewSizedMetaFile(sizeLimit int) *sizedMetaFile {
	return &sizedMetaFile{
		root: &backuppb.MetaFile{
			Schemas:   make([]*backuppb.Schema, 0),
			DataFiles: make([]*backuppb.File, 0),
			RawRanges: make([]*backuppb.RawRange, 0),
		},
		sizeLimit: sizeLimit,
	}
}

func (f *sizedMetaFile) append(file interface{}, op AppendOp) bool {
	// append to root
	// 	TODO maybe use multi level index
	size, itemCount := op.appendFile(f.root, file)
	f.itemNum += itemCount
	f.size += size
	// f.size would reset outside
	return f.size > f.sizeLimit
}

// MetaWriter represents wraps a writer, and the MetaWriter should be compatible with old version of backupmeta.
type MetaWriter struct {
	storage           storage.ExternalStorage
	metafileSizeLimit int
	// a flag to control whether we generate v1 or v2 meta.
	useV2Meta  bool
	backupMeta *backuppb.BackupMeta
	// used to generate MetaFile name.
	metafileSizes  map[string]int
	metafileSeqNum map[string]int
	metafiles      *sizedMetaFile
	// the start time of StartWriteMetas
	// it's use to calculate the time costs.
	start time.Time
	// wg waits StartWriterMetas exits
	wg sync.WaitGroup
	// internal item channel
	metasCh chan interface{}
	errCh   chan error

	// records the total item of in one write meta job.
	flushedItemNum int

	cipher *backuppb.CipherInfo
}

// NewMetaWriter creates MetaWriter.
func NewMetaWriter(storage storage.ExternalStorage,
	metafileSizeLimit int,
	useV2Meta bool, cipher *backuppb.CipherInfo) *MetaWriter {
	return &MetaWriter{
		start:             time.Now(),
		storage:           storage,
		metafileSizeLimit: metafileSizeLimit,
		useV2Meta:         useV2Meta,
		// keep the compatibility for old backupmeta.Ddls
		// old version: Ddls, _ := json.Marshal(make([]*model.Job, 0))
		backupMeta:     &backuppb.BackupMeta{Ddls: []byte("[]")},
		metafileSizes:  make(map[string]int),
		metafiles:      NewSizedMetaFile(metafileSizeLimit),
		metafileSeqNum: make(map[string]int),
		cipher:         cipher,
	}
}

func (writer *MetaWriter) reset() {
	writer.metasCh = make(chan interface{}, MaxBatchSize)
	writer.errCh = make(chan error)

	// reset flushedItemNum for next meta.
	writer.flushedItemNum = 0
}

// Update updates some property of backupmeta.
func (writer *MetaWriter) Update(f func(m *backuppb.BackupMeta)) {
	f(writer.backupMeta)
}

// Send sends the item to buffer.
func (writer *MetaWriter) Send(m interface{}, op AppendOp) error {
	select {
	case writer.metasCh <- m:
	// receive an error from StartWriteMetasAsync
	case err := <-writer.errCh:
		return errors.Trace(err)
	}
	return nil
}

func (writer *MetaWriter) close() {
	close(writer.metasCh)
}

// StartWriteMetasAsync writes four kind of meta into backupmeta.
// 1. file
// 2. schema
// 3. ddl
// 4. rawRange( raw kv )
// when useBackupMetaV2 enabled, it will generate multi-level index backupmetav2.
// else it will generate backupmeta as before for compatibility.
// User should call FinishWriteMetas after StartWriterMetasAsync.
func (writer *MetaWriter) StartWriteMetasAsync(ctx context.Context, op AppendOp) {
	writer.reset()
	writer.start = time.Now()
	writer.wg.Add(1)
	go func() {
		defer func() {
			close(writer.errCh)
			// close errCh before metaCh closed
			writer.wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				log.Info("exit write metas by context done")
				return
			case meta, ok := <-writer.metasCh:
				if !ok {
					log.Info("write metas finished", zap.String("type", op.name()))
					return
				}
				needFlush := writer.metafiles.append(meta, op)
				if writer.useV2Meta && needFlush {
					err := writer.flushMetasV2(ctx, op)
					if err != nil {
						writer.errCh <- err
					}
				}
			}
		}
	}()
}

// FinishWriteMetas close the channel in StartWriteMetasAsync and flush the buffered data.
func (writer *MetaWriter) FinishWriteMetas(ctx context.Context, op AppendOp) error {
	writer.close()
	// always start one goroutine to write one kind of meta.
	writer.wg.Wait()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("MetaWriter.Finish", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	var err error
	// flush the buffered meta
	if !writer.useV2Meta {
		writer.fillMetasV1(ctx, op)
	} else {
		err = writer.flushMetasV2(ctx, op)
		if err != nil {
			return errors.Trace(err)
		}
	}

	costs := time.Since(writer.start)
	if op == AppendDataFile {
		summary.CollectSuccessUnit("backup ranges", writer.flushedItemNum, costs)
	}
	log.Info("finish the write metas", zap.Int("item", writer.flushedItemNum),
		zap.String("type", op.name()), zap.Duration("costs", costs))
	return nil
}

// FlushBackupMeta flush the `backupMeta` to `ExternalStorage`
func (writer *MetaWriter) FlushBackupMeta(ctx context.Context) error {
	// Set schema version
	if writer.useV2Meta {
		writer.backupMeta.Version = MetaV2
	} else {
		writer.backupMeta.Version = MetaV1
	}

	// Flush the writer.backupMeta to storage
	backupMetaData, err := proto.Marshal(writer.backupMeta)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("backup meta", zap.Reflect("meta", writer.backupMeta))
	log.Info("save backup meta", zap.Int("size", len(backupMetaData)))

	encryptBuff, iv, err := Encrypt(backupMetaData, writer.cipher)
	if err != nil {
		return errors.Trace(err)
	}

	return writer.storage.WriteFile(ctx, MetaFile, append(iv, encryptBuff...))
}

// fillMetasV1 keep the compatibility for old version.
// for MetaV1, just put in backupMeta
func (writer *MetaWriter) fillMetasV1(_ context.Context, op AppendOp) {
	switch op {
	case AppendDataFile:
		writer.backupMeta.Files = writer.metafiles.root.DataFiles
	default:
		log.Panic("unsupport op type", zap.Any("op", op))
	}
	writer.flushedItemNum += writer.metafiles.itemNum
}

func (writer *MetaWriter) flushMetasV2(ctx context.Context, op AppendOp) error {
	var index *backuppb.MetaFile
	switch op {
	case AppendDataFile:
		if len(writer.metafiles.root.DataFiles) == 0 {
			return nil
		}
		// Add the metafile to backupmeta and reset metafiles.
		if writer.backupMeta.FileIndex == nil {
			writer.backupMeta.FileIndex = &backuppb.MetaFile{}
		}
		index = writer.backupMeta.FileIndex
	}
	content, err := writer.metafiles.root.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	name := op.name()
	writer.metafileSizes[name] += writer.metafiles.size
	// Flush metafiles to external storage.
	writer.metafileSeqNum["metafiles"] += 1
	fname := fmt.Sprintf("backupmeta.%s.%09d", name, writer.metafileSeqNum["metafiles"])

	encyptedContent, iv, err := Encrypt(content, writer.cipher)
	if err != nil {
		return errors.Trace(err)
	}

	if err = writer.storage.WriteFile(ctx, fname, encyptedContent); err != nil {
		return errors.Trace(err)
	}
	checksum := sha256.Sum256(content)
	file := &backuppb.File{
		Name:     fname,
		Sha256:   checksum[:],
		Size_:    uint64(len(content)),
		CipherIv: iv,
	}

	index.MetaFiles = append(index.MetaFiles, file)
	writer.flushedItemNum += writer.metafiles.itemNum
	writer.metafiles = NewSizedMetaFile(writer.metafiles.sizeLimit)
	return nil
}

// ArchiveSize represents the size of ArchiveSize.
func (writer *MetaWriter) ArchiveSize() uint64 {
	total := uint64(0)
	for _, file := range writer.backupMeta.Files {
		total += file.Size_
	}
	total += uint64(writer.metafileSizes["datafile"])
	return total
}

// Backupmeta clones a backupmeta.
func (writer *MetaWriter) Backupmeta() *backuppb.BackupMeta {
	clone := proto.Clone(writer.backupMeta)
	return clone.(*backuppb.BackupMeta)
}
