// automatically generated by the FlatBuffers compiler, do not modify

package banyandb.v1;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

import javax.annotation.Nullable;

@javax.annotation.Generated(value="flatc")
@SuppressWarnings("unused")
public final class Metadata extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Metadata getRootAsMetadata(ByteBuffer _bb) { return getRootAsMetadata(_bb, new Metadata()); }
  public static Metadata getRootAsMetadata(ByteBuffer _bb, Metadata obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Metadata __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public  @Nullable java.lang.String group() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer groupAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer groupInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public  @Nullable java.lang.String name() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

  public static int createMetadata(FlatBufferBuilder builder,
      int groupOffset,
      int nameOffset) {
    builder.startTable(2);
    Metadata.addName(builder, nameOffset);
    Metadata.addGroup(builder, groupOffset);
    return Metadata.endMetadata(builder);
  }

  public static void startMetadata(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addGroup(FlatBufferBuilder builder, int groupOffset) { builder.addOffset(0, groupOffset, 0); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(1, nameOffset, 0); }
  public static int endMetadata(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Metadata get(int j) { return get(new Metadata(), j); }
    public Metadata get(Metadata obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

