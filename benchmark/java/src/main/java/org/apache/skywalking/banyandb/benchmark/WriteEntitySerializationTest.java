/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.skywalking.banyandb.benchmark;

import banyandb.v1.EntityValue;
import banyandb.v1.Field;
import banyandb.v1.Metadata;
import banyandb.v1.ValueType;
import banyandb.v1.WriteEntity;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import org.apache.skywalking.banyandb.Write;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * # JMH version: 1.32
 * # VM version: JDK 1.8.0_292, OpenJDK 64-Bit Server VM, 25.292-b10
 * # VM invoker: /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/jre/bin/java
 * # VM options: -javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=55698:/Applications/IntelliJ IDEA.app/Contents/bin -Dfile.encoding=UTF-8
 * # Blackhole mode: full + dont-inline hint
 * # Warmup: 5 iterations, 10 s each
 * # Measurement: 5 iterations, 10 s each
 * # Timeout: 10 min per iteration
 * # Threads: 1 thread, will synchronize iterations
 * # Benchmark mode: Average time, time/op
 * <p>
 * Benchmark                                                                  Mode  Cnt     Score    Error   Units
 * WriteEntitySerializationTest.flatbuffers                                   avgt   25  3044.054 ± 34.837   ns/op
 * WriteEntitySerializationTest.flatbuffers:·gc.alloc.rate                    avgt   25   911.872 ± 10.301  MB/sec
 * WriteEntitySerializationTest.flatbuffers:·gc.alloc.rate.norm               avgt   25  3056.000 ±  0.001    B/op
 * WriteEntitySerializationTest.flatbuffers:·gc.churn.PS_Eden_Space           avgt   25   912.394 ± 10.261  MB/sec
 * WriteEntitySerializationTest.flatbuffers:·gc.churn.PS_Eden_Space.norm      avgt   25  3057.783 ± 10.009    B/op
 * WriteEntitySerializationTest.flatbuffers:·gc.churn.PS_Survivor_Space       avgt   25     0.190 ±  0.018  MB/sec
 * WriteEntitySerializationTest.flatbuffers:·gc.churn.PS_Survivor_Space.norm  avgt   25     0.637 ±  0.059    B/op
 * WriteEntitySerializationTest.flatbuffers:·gc.count                         avgt   25  3878.000           counts
 * WriteEntitySerializationTest.flatbuffers:·gc.time                          avgt   25  2168.000               ms
 * WriteEntitySerializationTest.protobuf                                      avgt   25   514.010 ± 12.638   ns/op
 * WriteEntitySerializationTest.protobuf:·gc.alloc.rate                       avgt   25  3833.648 ± 90.162  MB/sec
 * WriteEntitySerializationTest.protobuf:·gc.alloc.rate.norm                  avgt   25  2168.000 ±  0.001    B/op
 * WriteEntitySerializationTest.protobuf:·gc.churn.PS_Eden_Space              avgt   25  3835.530 ± 94.020  MB/sec
 * WriteEntitySerializationTest.protobuf:·gc.churn.PS_Eden_Space.norm         avgt   25  2168.989 ±  6.134    B/op
 * WriteEntitySerializationTest.protobuf:·gc.churn.PS_Survivor_Space          avgt   25     0.195 ±  0.020  MB/sec
 * WriteEntitySerializationTest.protobuf:·gc.churn.PS_Survivor_Space.norm     avgt   25     0.110 ±  0.011    B/op
 * WriteEntitySerializationTest.protobuf:·gc.count                            avgt   25  3629.000           counts
 * WriteEntitySerializationTest.protobuf:·gc.time                             avgt   25  2227.000               ms
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class WriteEntitySerializationTest {

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
            int o = WriteEntitySerializationTest.buildField(fbb, entity.items[i]);
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
                .include(WriteEntitySerializationTest.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .resultFormat(ResultFormatType.JSON)
//                .result("benchmark-result/" + System.currentTimeMillis() + ".json")
                .build();
        new Runner(opts).run();
    }
}
