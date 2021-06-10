package clientutil

import flatbuffers "github.com/google/flatbuffers/go"

type FlatBufferOffsetBuilder func(*flatbuffers.Builder) flatbuffers.UOffsetT
