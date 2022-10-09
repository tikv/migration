package model

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"reflect"

	"github.com/tinylib/msgp/msgp"
)

var (
	fieldNum           = getStructFieldNum(RawKVEntry{})
	fieldNameOpType    = generateFeildName("op_type")
	fieldNameKey       = generateFeildName("key")
	fieldNameValue     = generateFeildName("value")
	fieldNameOldValue  = generateFeildName("old_value")
	fieldNameStartTs   = generateFeildName("start_ts")
	fieldNameCRTs      = generateFeildName("crts")
	fieldNameExpiredTs = generateFeildName("expired_ts")
	fieldNameRegionID  = generateFeildName("region_id")
	fieldNameKeySpanID = generateFeildName("keyspan_id")
	fieldNameSequence  = generateFeildName("sequence")
	fieldNames         = [][]byte{
		fieldNameOpType,
		fieldNameKey,
		fieldNameValue,
		fieldNameOldValue,
		fieldNameStartTs,
		fieldNameCRTs,
		fieldNameExpiredTs,
		fieldNameRegionID,
		fieldNameKeySpanID,
		fieldNameSequence,
	}
)

// DecodeMsg implements msgp.Decodable
func (z *OpType) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 int
		zb0001, err = dc.ReadInt()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = OpType(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z OpType) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteInt(int(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z OpType) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendInt(o, int(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *OpType) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 int
		zb0001, bts, err = msgp.ReadIntBytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = OpType(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z OpType) Msgsize() (s int) {
	s = msgp.IntSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RawKVEntry) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "op_type":
			{
				var zb0002 int
				zb0002, err = dc.ReadInt()
				if err != nil {
					err = msgp.WrapError(err, "OpType")
					return
				}
				z.OpType = OpType(zb0002)
			}
		case "key":
			z.Key, err = dc.ReadBytes(z.Key)
			if err != nil {
				err = msgp.WrapError(err, "Key")
				return
			}
		case "value":
			z.Value, err = dc.ReadBytes(z.Value)
			if err != nil {
				err = msgp.WrapError(err, "Value")
				return
			}
		case "old_value":
			z.OldValue, err = dc.ReadBytes(z.OldValue)
			if err != nil {
				err = msgp.WrapError(err, "OldValue")
				return
			}
		case "start_ts":
			z.StartTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "crts":
			z.CRTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "CRTs")
				return
			}
		case "expired_ts":
			z.ExpiredTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ExpiredTs")
				return
			}
		case "region_id":
			z.RegionID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "RegionID")
				return
			}
		case "keyspan_id":
			z.KeySpanID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "KeySpanID")
				return
			}
		case "sequence":
			z.Sequence, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Sequence")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *RawKVEntry) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 10
	err = en.Append(0x80 + byte(fieldNum))
	if err != nil {
		return
	}
	// write "op_type"
	err = en.Append(fieldNameOpType...)
	if err != nil {
		return
	}
	err = en.WriteInt(int(z.OpType))
	if err != nil {
		err = msgp.WrapError(err, "OpType")
		return
	}
	// write "key"
	err = en.Append(fieldNameKey...)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.Key)
	if err != nil {
		err = msgp.WrapError(err, "Key")
		return
	}
	// write "value"
	err = en.Append(fieldNameValue...)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.Value)
	if err != nil {
		err = msgp.WrapError(err, "Value")
		return
	}
	// write "old_value"
	err = en.Append(fieldNameOldValue...)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.OldValue)
	if err != nil {
		err = msgp.WrapError(err, "OldValue")
		return
	}
	// write "start_ts"
	err = en.Append(fieldNameStartTs...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.StartTs)
	if err != nil {
		err = msgp.WrapError(err, "StartTs")
		return
	}
	// write "crts"
	err = en.Append(fieldNameCRTs...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.CRTs)
	if err != nil {
		err = msgp.WrapError(err, "CRTs")
		return
	}
	// write "expired_ts"
	err = en.Append(fieldNameExpiredTs...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ExpiredTs)
	if err != nil {
		err = msgp.WrapError(err, "ExpiredTs")
		return
	}
	// write "region_id"
	err = en.Append(fieldNameRegionID...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.RegionID)
	if err != nil {
		err = msgp.WrapError(err, "RegionID")
		return
	}
	// write "keyspan_id"
	err = en.Append(fieldNameKeySpanID...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.KeySpanID)
	if err != nil {
		err = msgp.WrapError(err, "KeySpanID")
		return
	}
	// write "sequence"
	err = en.Append(fieldNameSequence...)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Sequence)
	if err != nil {
		err = msgp.WrapError(err, "Sequence")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *RawKVEntry) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 10
	o = append(o, 0x80+byte(fieldNum))
	// string "op_type"
	o = append(o, fieldNameOpType...)
	o = msgp.AppendInt(o, int(z.OpType))
	// string "key"
	o = append(o, fieldNameKey...)
	o = msgp.AppendBytes(o, z.Key)
	// string "value"
	o = append(o, fieldNameValue...)
	o = msgp.AppendBytes(o, z.Value)
	// string "old_value"
	o = append(o, fieldNameOldValue...)
	o = msgp.AppendBytes(o, z.OldValue)
	// string "start_ts"
	o = append(o, fieldNameStartTs...)
	o = msgp.AppendUint64(o, z.StartTs)
	// string "crts"
	o = append(o, fieldNameCRTs...)
	o = msgp.AppendUint64(o, z.CRTs)
	// string "expired_ts"
	o = append(o, fieldNameExpiredTs...)
	o = msgp.AppendUint64(o, z.ExpiredTs)
	// string "region_id"
	o = append(o, fieldNameRegionID...)
	o = msgp.AppendUint64(o, z.RegionID)
	// string "keyspan_id"
	o = append(o, fieldNameKeySpanID...)
	o = msgp.AppendUint64(o, z.KeySpanID)
	// string "sequence"
	o = append(o, fieldNameSequence...)
	o = msgp.AppendUint64(o, z.Sequence)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *RawKVEntry) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "op_type":
			{
				var zb0002 int
				zb0002, bts, err = msgp.ReadIntBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "OpType")
					return
				}
				z.OpType = OpType(zb0002)
			}
		case "key":
			z.Key, bts, err = msgp.ReadBytesBytes(bts, z.Key)
			if err != nil {
				err = msgp.WrapError(err, "Key")
				return
			}
		case "value":
			z.Value, bts, err = msgp.ReadBytesBytes(bts, z.Value)
			if err != nil {
				err = msgp.WrapError(err, "Value")
				return
			}
		case "old_value":
			z.OldValue, bts, err = msgp.ReadBytesBytes(bts, z.OldValue)
			if err != nil {
				err = msgp.WrapError(err, "OldValue")
				return
			}
		case "start_ts":
			z.StartTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "crts":
			z.CRTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CRTs")
				return
			}
		case "expired_ts":
			z.ExpiredTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExpiredTs")
				return
			}
		case "region_id":
			z.RegionID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "RegionID")
				return
			}
		case "keyspan_id":
			z.KeySpanID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "KeySpanID")
				return
			}
		case "sequence":
			z.Sequence, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Sequence")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *RawKVEntry) Msgsize() (s int) {
	s = 1
	for _, fieldName := range fieldNames {
		s += len(fieldName)
	}
	s += msgp.IntSize + // OpType
		msgp.BytesPrefixSize + len(z.Key) + // Key
		msgp.BytesPrefixSize + len(z.Value) + // Value
		msgp.BytesPrefixSize + len(z.OldValue) + // OldVale
		msgp.Uint64Size + // StartTs
		msgp.Uint64Size + // CRTs
		msgp.Uint64Size + // ExpiredTs
		msgp.Uint64Size + // RegionID
		msgp.Uint64Size + // KeySpanID
		msgp.Uint64Size // Sequence
	return
}

func generateFeildName(feildName string) (bytes []byte) {
	l := len(feildName)
	bytes = make([]byte, 0, l+1)
	bytes = append(bytes, 0xa0+byte(l))
	bytes = append(bytes, []byte(feildName)...)
	return
}

func getStructFieldNum(structName interface{}) int {
	t := reflect.TypeOf(structName)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.NumField()
}
