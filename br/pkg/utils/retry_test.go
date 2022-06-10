package utils

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableError(t *testing.T) {
	require.False(t, IsRetryableError(context.Canceled))
	require.False(t, IsRetryableError(context.DeadlineExceeded))
	require.False(t, IsRetryableError(io.EOF))
	require.False(t, IsRetryableError(&net.AddrError{}))
	require.False(t, IsRetryableError(&net.DNSError{}))
	require.True(t, IsRetryableError(&net.DNSError{IsTimeout: true}))

	// gRPC Errors
	require.False(t, IsRetryableError(status.Error(codes.Canceled, "")))
	require.True(t, IsRetryableError(status.Error(codes.Unknown, "")))
	require.True(t, IsRetryableError(status.Error(codes.DeadlineExceeded, "")))
	require.True(t, IsRetryableError(status.Error(codes.NotFound, "")))
	require.True(t, IsRetryableError(status.Error(codes.AlreadyExists, "")))
	require.True(t, IsRetryableError(status.Error(codes.PermissionDenied, "")))
	require.True(t, IsRetryableError(status.Error(codes.ResourceExhausted, "")))
	require.True(t, IsRetryableError(status.Error(codes.Aborted, "")))
	require.True(t, IsRetryableError(status.Error(codes.OutOfRange, "")))
	require.True(t, IsRetryableError(status.Error(codes.Unavailable, "")))
	require.True(t, IsRetryableError(status.Error(codes.DataLoss, "")))

	// sqlmock errors
	require.False(t, IsRetryableError(fmt.Errorf("call to database Close was not expected")))
	require.True(t, IsRetryableError(errors.New("call to database Close was not expected")))

	// multierr
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, context.Canceled)))
	require.True(t, IsRetryableError(multierr.Combine(&net.DNSError{IsTimeout: true}, &net.DNSError{IsTimeout: true})))
	require.False(t, IsRetryableError(multierr.Combine(context.Canceled, &net.DNSError{IsTimeout: true})))
}
