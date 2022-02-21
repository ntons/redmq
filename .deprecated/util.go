package redmq

import (
	"reflect"
	"unsafe"
)

func b2s(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

func s2b(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s
	(*reflect.SliceHeader)(unsafe.Pointer(&b)).Cap = len(s)
	return
}
