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

type autoCreateOption struct {
	autoCreate bool
}

func WithAutoCreate(autoCreate bool) SendOption {
	return autoCreateOption{autoCreate: autoCreate}
}
func (x autoCreateOption) apply(o *sendOptions) {
	o.autoCreate = x.autoCreate
}
