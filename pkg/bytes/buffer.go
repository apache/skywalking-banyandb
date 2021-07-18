package bytes

type ByteBuf struct {
	b []byte
}

func (b *ByteBuf) Len() int {
	return len(b.b)
}

func (b *ByteBuf) Bytes() []byte {
	return b.b
}

func (b *ByteBuf) Append(raw []byte) (int, error) {
	b.b = append(b.b, raw...)
	return len(raw), nil
}

func (b *ByteBuf) AppendByte(ch byte) (int, error) {
	b.b = append(b.b, ch)
	return 1, nil
}

func (b *ByteBuf) AppendString(s string) (int, error) {
	b.b = append(b.b, s...)
	return len(s), nil
}

// AppendUInt64 appends uint64 with BigEndianness
func (b *ByteBuf) AppendUInt64(v uint64) (int, error) {
	b.b = append(b.b, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	return 8, nil
}

// Reset the underlying slice while keeping the capacity
func (b *ByteBuf) Reset() {
	b.b = b.b[:0]
}
