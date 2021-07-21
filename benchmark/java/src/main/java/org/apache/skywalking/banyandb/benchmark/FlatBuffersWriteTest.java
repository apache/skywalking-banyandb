package org.apache.skywalking.banyandb.benchmark;

import banyandb.v1.*;
import com.google.flatbuffers.FlatBufferBuilder;
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
 * # Benchmark: org.apache.skywalking.banyandb.benchmark.FlatBuffersWriteTest.writeEntity
 * <p>
 * Benchmark                                                          Mode  Cnt     Score     Error   Units
 * FlatBuffersWriteTest.writeEntity                                   avgt   25  4008,724 ± 451,721   ns/op
 * FlatBuffersWriteTest.writeEntity:·gc.alloc.rate                    avgt   25   703,532 ±  60,040  MB/sec
 * FlatBuffersWriteTest.writeEntity:·gc.alloc.rate.norm               avgt   25  3056,000 ±   0,001    B/op
 * FlatBuffersWriteTest.writeEntity:·gc.churn.PS_Eden_Space           avgt   25   703,281 ±  61,514  MB/sec
 * FlatBuffersWriteTest.writeEntity:·gc.churn.PS_Eden_Space.norm      avgt   25  3053,959 ±  23,092    B/op
 * FlatBuffersWriteTest.writeEntity:·gc.churn.PS_Survivor_Space       avgt   25     0,124 ±   0,032  MB/sec
 * FlatBuffersWriteTest.writeEntity:·gc.churn.PS_Survivor_Space.norm  avgt   25     0,526 ±   0,110    B/op
 * FlatBuffersWriteTest.writeEntity:·gc.count                         avgt   25  2364,000            counts
 * FlatBuffersWriteTest.writeEntity:·gc.time                          avgt   25  2002,000                ms
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
        bh.consume(WriteEntity.getRootAsWriteEntity(fbb.dataBuffer()));
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
                .result("benchmark-result/" + System.currentTimeMillis() + ".json")
                .build();
        new Runner(opts).run();
    }
}
