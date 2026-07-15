package main

import (
	"fmt"

	pb "github.com/AMDL-Web/wrapper-manager/proto"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// managerCodec keeps the normal protobuf codec for every message except a
// successful decrypt reply. That reply ends with the large sample field, so it
// can be represented as a small protobuf prefix plus the existing sample bytes.
type managerCodec struct {
	proto encoding.CodecV2
}

func newManagerCodec() (*managerCodec, error) {
	codec := encoding.GetCodecV2("proto")
	if codec == nil {
		return nil, fmt.Errorf("protobuf gRPC codec is not registered")
	}
	return &managerCodec{proto: codec}, nil
}

func (c *managerCodec) Name() string {
	return c.proto.Name()
}

func (c *managerCodec) Marshal(value any) (mem.BufferSlice, error) {
	if reply, ok := value.(*pb.DecryptReply); ok {
		if buffers, fast := marshalDecryptSuccess(reply); fast {
			return buffers, nil
		}
	}
	return c.proto.Marshal(value)
}

func (c *managerCodec) Unmarshal(data mem.BufferSlice, value any) error {
	return c.proto.Unmarshal(data, value)
}

func marshalDecryptSuccess(reply *pb.DecryptReply) (mem.BufferSlice, bool) {
	if reply == nil || reply.Header == nil || reply.Data == nil ||
		reply.Header.Code != 0 || reply.Header.Msg != "SUCCESS" || len(reply.Data.Sample) == 0 {
		return nil, false
	}

	headerLength := protowire.SizeTag(2) + protowire.SizeBytes(len(reply.Header.Msg))
	dataPrefixLength := protowire.SizeTag(4) + protowire.SizeVarint(uint64(len(reply.Data.Sample)))
	if reply.Data.AdamId != "" {
		dataPrefixLength += protowire.SizeTag(1) + protowire.SizeBytes(len(reply.Data.AdamId))
	}
	if reply.Data.Key != "" {
		dataPrefixLength += protowire.SizeTag(2) + protowire.SizeBytes(len(reply.Data.Key))
	}
	if reply.Data.SampleIndex != 0 {
		dataPrefixLength += protowire.SizeTag(3) + protowire.SizeVarint(uint64(int64(reply.Data.SampleIndex)))
	}

	dataLength := dataPrefixLength + len(reply.Data.Sample)
	prefixLength := protowire.SizeTag(1) + protowire.SizeVarint(uint64(headerLength)) + headerLength +
		protowire.SizeTag(2) + protowire.SizeVarint(uint64(dataLength)) + dataPrefixLength
	prefix := make([]byte, 0, prefixLength)
	prefix = protowire.AppendTag(prefix, 1, protowire.BytesType)
	prefix = protowire.AppendVarint(prefix, uint64(headerLength))
	prefix = protowire.AppendTag(prefix, 2, protowire.BytesType)
	prefix = protowire.AppendString(prefix, reply.Header.Msg)
	prefix = protowire.AppendTag(prefix, 2, protowire.BytesType)
	prefix = protowire.AppendVarint(prefix, uint64(dataLength))
	if reply.Data.AdamId != "" {
		prefix = protowire.AppendTag(prefix, 1, protowire.BytesType)
		prefix = protowire.AppendString(prefix, reply.Data.AdamId)
	}
	if reply.Data.Key != "" {
		prefix = protowire.AppendTag(prefix, 2, protowire.BytesType)
		prefix = protowire.AppendString(prefix, reply.Data.Key)
	}
	if reply.Data.SampleIndex != 0 {
		prefix = protowire.AppendTag(prefix, 3, protowire.VarintType)
		prefix = protowire.AppendVarint(prefix, uint64(int64(reply.Data.SampleIndex)))
	}
	prefix = protowire.AppendTag(prefix, 4, protowire.BytesType)
	prefix = protowire.AppendVarint(prefix, uint64(len(reply.Data.Sample)))

	// This also makes the fast path automatically fall back if generated code
	// later adds a populated field or any nested message carries unknown fields.
	if len(prefix) != prefixLength || len(prefix)+len(reply.Data.Sample) != proto.Size(reply) {
		return nil, false
	}
	return mem.BufferSlice{
		mem.SliceBuffer(prefix),
		mem.SliceBuffer(reply.Data.Sample),
	}, true
}
