package org.apache.skywalking.banyandb.benchmark;

import banyandb.v1.*;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import org.apache.skywalking.banyandb.Write;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.String;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * # JMH version: 1.32
 * # VM version: JDK 1.8.0_292, OpenJDK 64-Bit Server VM, 25.292-b10
 * # VM invoker: /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/bin/java
 * # VM options: -Dvisualvm.id=456817831633044 -javaagent:/Users/megrez/Library/Application Support/JetBrains/Toolbox/apps/IDEA-U/ch-0/211.7628.21/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=64278:/Users/megrez/Library/Application Support/JetBrains/Toolbox/apps/IDEA-U/ch-0/211.7628.21/IntelliJ IDEA.app/Contents/bin -Dfile.encoding=UTF-8
 * # Blackhole mode: full + dont-inline hint
 * # Warmup: 5 iterations, 10 s each
 * # Measurement: 5 iterations, 10 s each
 * # Timeout: 10 min per iteration
 * # Threads: 1 thread, will synchronize iterations
 * # Benchmark mode: Average time, time/op
 * <p>
 * Benchmark                                                          Mode  Cnt     Score     Error   Units
 * FlatBuffersWriteTest.flatbuffers                                   avgt   25  3780,360 ± 272,301   ns/op
 * FlatBuffersWriteTest.flatbuffers:·gc.alloc.rate                    avgt   25   739,601 ±  45,059  MB/sec
 * FlatBuffersWriteTest.flatbuffers:·gc.alloc.rate.norm               avgt   25  3056,000 ±   0,001    B/op
 * FlatBuffersWriteTest.flatbuffers:·gc.churn.PS_Eden_Space           avgt   25   740,157 ±  47,014  MB/sec
 * FlatBuffersWriteTest.flatbuffers:·gc.churn.PS_Eden_Space.norm      avgt   25  3057,320 ±  21,074    B/op
 * FlatBuffersWriteTest.flatbuffers:·gc.churn.PS_Survivor_Space       avgt   25     0,130 ±   0,035  MB/sec
 * FlatBuffersWriteTest.flatbuffers:·gc.churn.PS_Survivor_Space.norm  avgt   25     0,527 ±   0,126    B/op
 * FlatBuffersWriteTest.flatbuffers:·gc.count                         avgt   25  2529,000            counts
 * FlatBuffersWriteTest.flatbuffers:·gc.time                          avgt   25  1917,000                ms
 * FlatBuffersWriteTest.protobuf                                      avgt   25   652,362 ±  46,437   ns/op
 * FlatBuffersWriteTest.protobuf:·gc.alloc.rate                       avgt   25  2922,121 ± 187,469  MB/sec
 * FlatBuffersWriteTest.protobuf:·gc.alloc.rate.norm                  avgt   25  2089,600 ± 119,878    B/op
 * FlatBuffersWriteTest.protobuf:·gc.churn.PS_Eden_Space              avgt   25  2920,744 ± 187,923  MB/sec
 * FlatBuffersWriteTest.protobuf:·gc.churn.PS_Eden_Space.norm         avgt   25  2088,672 ± 120,670    B/op
 * FlatBuffersWriteTest.protobuf:·gc.churn.PS_Survivor_Space          avgt   25     0,155 ±   0,033  MB/sec
 * FlatBuffersWriteTest.protobuf:·gc.churn.PS_Survivor_Space.norm     avgt   25     0,109 ±   0,019    B/op
 * FlatBuffersWriteTest.protobuf:·gc.count                            avgt   25  2671,000            counts
 * FlatBuffersWriteTest.protobuf:·gc.time                             avgt   25  2069,000                ms
 */
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
    public void flatbuffers(Blackhole bh, EntityModel entity) {
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
        bh.consume(WriteEntity.getRootAsWriteEntity(fbb.dataBuffer()));
    }

    @Benchmark
    public void protobuf(Blackhole bh, EntityModel entity) {
        Write.Metadata metadata = Write.Metadata.newBuilder().setGroup(entity.group).setName(entity.name).build();
        Write.EntityValue.Builder entityValueBuilder = Write.EntityValue.newBuilder()
                .setEntityId(entity.entityID)
                .setDataBinary(ByteString.copyFrom(entity.binary))
                .setTimestampNanoseconds(entity.ts);
        for (int i = 0; i < entity.items.length; i++) {
            entityValueBuilder.addFields(i, buildField(entity.items[i]));
        }
        Write.WriteEntity writeEntity = Write.WriteEntity.newBuilder()
                .setMetaData(metadata)
                .setEntity(entityValueBuilder)
                .build();
        bh.consume(writeEntity);
    }

    public static Write.Field buildField(Object obj) {
        if (obj == null) {
            return Write.Field.newBuilder().setNull(NullValue.NULL_VALUE).build();
        }
        if (obj instanceof Integer) {
            Write.Int writeInt = Write.Int.newBuilder().setValue(((Integer) obj)).build();
            return Write.Field.newBuilder().setInt(writeInt).build();
        } else if (obj instanceof Long) {
            Write.Int writeInt = Write.Int.newBuilder().setValue(((Long) obj)).build();
            return Write.Field.newBuilder().setInt(writeInt).build();
        } else if (obj instanceof String) {
            Write.Str writeStr = Write.Str.newBuilder().setValue((String) obj).build();
            return Write.Field.newBuilder().setStr(writeStr).build();
        } else {
            throw new IllegalArgumentException("type is not supported");
        }
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
            valType = ValueType.Str;
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
        return banyandb.v1.Str.createStr(fbb, fbb.createString(str));
    }

    public static void main(String[] args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(FlatBuffersWriteTest.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .resultFormat(ResultFormatType.JSON)
//                .result("benchmark-result/" + System.currentTimeMillis() + ".json")
                .build();
        new Runner(opts).run();
    }
}
