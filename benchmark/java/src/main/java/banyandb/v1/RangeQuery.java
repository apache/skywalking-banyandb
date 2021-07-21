// automatically generated by the FlatBuffers compiler, do not modify

package banyandb.v1;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@javax.annotation.Generated(value="flatc")
@SuppressWarnings("unused")
public final class RangeQuery extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static RangeQuery getRootAsRangeQuery(ByteBuffer _bb) { return getRootAsRangeQuery(_bb, new RangeQuery()); }
  public static RangeQuery getRootAsRangeQuery(ByteBuffer _bb, RangeQuery obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public RangeQuery __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long begin() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public long end() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }

  public static int createRangeQuery(FlatBufferBuilder builder,
      long begin,
      long end) {
    builder.startTable(2);
    RangeQuery.addEnd(builder, end);
    RangeQuery.addBegin(builder, begin);
    return RangeQuery.endRangeQuery(builder);
  }

  public static void startRangeQuery(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addBegin(FlatBufferBuilder builder, long begin) { builder.addLong(0, begin, 0L); }
  public static void addEnd(FlatBufferBuilder builder, long end) { builder.addLong(1, end, 0L); }
  public static int endRangeQuery(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public RangeQuery get(int j) { return get(new RangeQuery(), j); }
    public RangeQuery get(RangeQuery obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

