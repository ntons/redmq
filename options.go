package redmq

type SendOption interface {
	apply(*sendOptions)
}

type sendOptions struct {
	maxLen     int64
	autoCreate bool
}

type maxLenOption struct {
	maxLen int64
}

func WithMaxLen(maxLen int64) SendOption {
	return maxLenOption{maxLen: maxLen}
}
func (x maxLenOption) apply(o *sendOptions) {
	o.maxLen = x.maxLen
}

type autoCreateOption struct{}

func WithAutoCreate() SendOption {
	return autoCreateOption{}
}
func (x autoCreateOption) apply(o *sendOptions) {
	o.autoCreate = true
}
