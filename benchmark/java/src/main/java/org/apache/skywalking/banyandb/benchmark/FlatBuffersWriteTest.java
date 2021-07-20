package org.apache.skywalking.banyandb.benchmark;

import banyandb.v1.*;
import com.google.flatbuffers.FlatBufferBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.String;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FlatBuffersWriteTest {

    @State(Scope.Benchmark)
    public static class EntityModel {
        public String entityID = "1";
        public String group = "default";
        public String name = "sw";
        public byte[] binary = new byte[]{14};
        public long ts = Double.valueOf(Instant.now().getEpochSecond() * 1E9).longValue();
        public Object[] items = new Object[]{"trace_id-xxfff.111323", 0, "webapp_id", "10.0.0.1_id", "/home_id", "webapp", "10.0.0.1", "/home", 300, 1622933202000000000L};
    }

    @Benchmark
    public void writeEntity(Blackhole bh, EntityModel entity) {
        FlatBufferBuilder fbb = new FlatBufferBuilder(1);
        int group = fbb.createString(entity.group);
        int name = fbb.createString(entity.name);
        int metadata = Metadata.createMetadata(fbb, group, name);
        int entityID = fbb.createString(entity.entityID);
        int binaryOffset = fbb.createByteVector(entity.binary);
        int[] fieldOffsets = new int[entity.items.length];
        for (int i = 0; i < entity.items.length; i++) {
            int o = FlatBuffersWriteTest.buildField(fbb, entity.items[i]);
            fieldOffsets[i] = o;
        }
        int fieldsVector = EntityValue.createFieldsVector(fbb, fieldOffsets);
        int entityValue = EntityValue.createEntityValue(fbb, entityID, entity.ts, binaryOffset, fieldsVector);
        int writeEntity = WriteEntity.createWriteEntity(fbb, metadata, entityValue);
        fbb.finish(writeEntity);
        bh.consume(fbb.sizedByteArray());
    }

    public static int buildField(FlatBufferBuilder fbb, Object obj) {
        if (obj == null) {
            Field.startField(fbb);
            Field.addValueType(fbb, ValueType.NONE);
            return Field.endField(fbb);
        }

        int valueTypeOffset;
        byte valType;
        if (obj instanceof String) {
            valueTypeOffset = buildString(fbb, (String) obj);
            valType = ValueType.String;
        } else if (obj instanceof Long) {
            valueTypeOffset = buildInt(fbb, (Long) obj);
            valType = ValueType.Int;
        } else if (obj instanceof Integer) {
            valueTypeOffset = buildInt(fbb, (Integer) obj);
            valType = ValueType.Int;
        } else {
            throw new IllegalArgumentException("type is not supported");
        }

        Field.startField(fbb);
        Field.addValueType(fbb, valType);
        Field.addValue(fbb, valueTypeOffset);
        return Field.endField(fbb);
    }

    public static int buildInt(FlatBufferBuilder fbb, long number) {
        return banyandb.v1.Int.createInt(fbb, number);
    }

    public static int buildString(FlatBufferBuilder fbb, String str) {
        return banyandb.v1.String.createString(fbb, fbb.createString(str));
    }

    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .addProfiler()
                .addProfiler(GCProfiler.class)
                .build();
        new Runner(opts).run();
    }
}
