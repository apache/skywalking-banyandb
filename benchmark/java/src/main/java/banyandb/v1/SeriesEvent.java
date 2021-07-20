// automatically generated by the FlatBuffers compiler, do not modify

package banyandb.v1;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

import javax.annotation.Nullable;

@javax.annotation.Generated(value="flatc")
@SuppressWarnings("unused")
public final class SeriesEvent extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static SeriesEvent getRootAsSeriesEvent(ByteBuffer _bb) { return getRootAsSeriesEvent(_bb, new SeriesEvent()); }
  public static SeriesEvent getRootAsSeriesEvent(ByteBuffer _bb, SeriesEvent obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public SeriesEvent __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public  @Nullable banyandb.v1.Metadata series() { return series(new banyandb.v1.Metadata()); }
  public  @Nullable banyandb.v1.Metadata series(banyandb.v1.Metadata obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  public java.lang.String fieldNamesCompositeSeriesId(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int fieldNamesCompositeSeriesIdLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public StringVector fieldNamesCompositeSeriesIdVector() { return fieldNamesCompositeSeriesIdVector(new StringVector()); }
  public StringVector fieldNamesCompositeSeriesIdVector(StringVector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  public byte action() { int o = __offset(8); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public long time() { int o = __offset(10); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }

  public static int createSeriesEvent(FlatBufferBuilder builder,
      int seriesOffset,
      int field_names_composite_series_idOffset,
      byte action,
      long time) {
    builder.startTable(4);
    SeriesEvent.addTime(builder, time);
    SeriesEvent.addFieldNamesCompositeSeriesId(builder, field_names_composite_series_idOffset);
    SeriesEvent.addSeries(builder, seriesOffset);
    SeriesEvent.addAction(builder, action);
    return SeriesEvent.endSeriesEvent(builder);
  }

  public static void startSeriesEvent(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addSeries(FlatBufferBuilder builder, int seriesOffset) { builder.addOffset(0, seriesOffset, 0); }
  public static void addFieldNamesCompositeSeriesId(FlatBufferBuilder builder, int fieldNamesCompositeSeriesIdOffset) { builder.addOffset(1, fieldNamesCompositeSeriesIdOffset, 0); }
  public static int createFieldNamesCompositeSeriesIdVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startFieldNamesCompositeSeriesIdVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addAction(FlatBufferBuilder builder, byte action) { builder.addByte(2, action, 0); }
  public static void addTime(FlatBufferBuilder builder, long time) { builder.addLong(3, time, 0L); }
  public static int endSeriesEvent(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public SeriesEvent get(int j) { return get(new SeriesEvent(), j); }
    public SeriesEvent get(SeriesEvent obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

