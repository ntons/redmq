package redmq

type CompressionType int

const (
	NoCompression CompressionType = iota
	LZ4
	ZLIB
)

type CompressionLevel int

const (
	// Default compression level
	Default CompressionLevel = iota

	// Faster compression, with lower compression ration
	Faster

	// Higher compression rate, but slower
	Better
)
