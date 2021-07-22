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
import banyandb.v1.Metadata;
import banyandb.v1.WriteEntity;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import org.apache.skywalking.banyandb.Write;

public class SizeCheckTest {
    public static long calculateFlatbuffersSize(WriteEntitySerializationTest.EntityModel entity) {
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
        // The data in this ByteBuffer does NOT start at 0, but at buf.position().
        // The number of bytes is buf.remaining().
        return fbb.dataBuffer().remaining();
    }

    public static long calculateProtobufSize(WriteEntitySerializationTest.EntityModel entity) {
        Write.Metadata metadata = Write.Metadata.newBuilder().setGroup(entity.group).setName(entity.name).build();
        Write.EntityValue.Builder entityValueBuilder = Write.EntityValue.newBuilder()
                .setEntityId(entity.entityID)
                .setDataBinary(ByteString.copyFrom(entity.binary))
                .setTimestampNanoseconds(entity.ts);
        for (int i = 0; i < entity.items.length; i++) {
            entityValueBuilder.addFields(i, WriteEntitySerializationTest.buildField(entity.items[i]));
        }
        Write.WriteEntity writeEntity = Write.WriteEntity.newBuilder()
                .setMetaData(metadata)
                .setEntity(entityValueBuilder)
                .build();
        return writeEntity.getSerializedSize();
    }

    public static void main(String[] args) {
        System.out.println("Flatbuffers: " + calculateFlatbuffersSize(new WriteEntitySerializationTest.EntityModel()));
        System.out.println("Protobuf: " + calculateProtobufSize(new WriteEntitySerializationTest.EntityModel()));
    }
}
