// automatically generated by the FlatBuffers compiler, do not modify

package banyandb.v1;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

import javax.annotation.Nullable;

@javax.annotation.Generated(value="flatc")
@SuppressWarnings("unused")
public final class WriteEntity extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static WriteEntity getRootAsWriteEntity(ByteBuffer _bb) { return getRootAsWriteEntity(_bb, new WriteEntity()); }
  public static WriteEntity getRootAsWriteEntity(ByteBuffer _bb, WriteEntity obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public WriteEntity __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public  @Nullable banyandb.v1.Metadata metaData() { return metaData(new banyandb.v1.Metadata()); }
  public  @Nullable banyandb.v1.Metadata metaData(banyandb.v1.Metadata obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  public  @Nullable banyandb.v1.EntityValue entity() { return entity(new banyandb.v1.EntityValue()); }
  public  @Nullable banyandb.v1.EntityValue entity(banyandb.v1.EntityValue obj) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }

  public static int createWriteEntity(FlatBufferBuilder builder,
      int meta_dataOffset,
      int entityOffset) {
    builder.startTable(2);
    WriteEntity.addEntity(builder, entityOffset);
    WriteEntity.addMetaData(builder, meta_dataOffset);
    return WriteEntity.endWriteEntity(builder);
  }

  public static void startWriteEntity(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addMetaData(FlatBufferBuilder builder, int metaDataOffset) { builder.addOffset(0, metaDataOffset, 0); }
  public static void addEntity(FlatBufferBuilder builder, int entityOffset) { builder.addOffset(1, entityOffset, 0); }
  public static int endWriteEntity(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public WriteEntity get(int j) { return get(new WriteEntity(), j); }
    public WriteEntity get(WriteEntity obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

