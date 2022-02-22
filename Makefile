.PHONY: all proto

all: proto

proto: msg.proto
	protoc --go_out=. $<

