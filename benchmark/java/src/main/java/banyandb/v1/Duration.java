// automatically generated by the FlatBuffers compiler, do not modify

package banyandb.v1;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@javax.annotation.Generated(value="flatc")
@SuppressWarnings("unused")
public final class Duration extends Struct {
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Duration __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long val() { return (long)bb.getInt(bb_pos + 0) & 0xFFFFFFFFL; }
  public byte unit() { return bb.get(bb_pos + 4); }

  public static int createDuration(FlatBufferBuilder builder, long val, byte unit) {
    builder.prep(4, 8);
    builder.pad(3);
    builder.putByte(unit);
    builder.putInt((int)val);
    return builder.offset();
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Duration get(int j) { return get(new Duration(), j); }
    public Duration get(Duration obj, int j) {  return obj.__assign(__element(j), bb); }
  }
}

