package redmq

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errUnauthenticated = status.Errorf(codes.Unauthenticated, "unauthenticated")
	errInvalidArgument = status.Errorf(codes.InvalidArgument, "invalid argument")
)

func newUnavailableError(format string, a ...interface{}) error {
	return status.Errorf(codes.Unavailable, format, a...)
}
