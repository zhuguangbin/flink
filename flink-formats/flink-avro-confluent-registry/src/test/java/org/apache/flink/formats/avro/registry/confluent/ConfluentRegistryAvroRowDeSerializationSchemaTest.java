/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;

/**
 * Test for the Confluent Avro serialization and deserialization schema.
 */
public class ConfluentRegistryAvroRowDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserialize() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final Schema schema = testData.f1.getSchema();
		final String schemaString = schema.toString();
		final SchemaCoder.SchemaCoderProvider provider = () -> new SchemaCoder() {
			@Override
			public Schema readSchema(InputStream in) {
				return schema;
			}

			@Override
			public void writeSchema(Schema schema, OutputStream out) throws IOException {
				//do nothing
			}
		};

		final ConfluentRegistryAvroRowSerializationSchema serializationSchema =
				new ConfluentRegistryAvroRowSerializationSchema(schemaString, provider);
		final ConfluentRegistryAvroRowDeserializationSchema deserializationSchema =
				new ConfluentRegistryAvroRowDeserializationSchema(schemaString, provider);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final Schema schema = testData.f1.getSchema();
		final String schemaString = schema.toString();
		final SchemaCoder.SchemaCoderProvider provider = () -> new SchemaCoder() {
			@Override
			public Schema readSchema(InputStream in) {
				return schema;
			}

			@Override
			public void writeSchema(Schema schema, OutputStream out) throws IOException {
				//do nothing
			}
		};

		final ConfluentRegistryAvroRowSerializationSchema serializationSchema =
				new ConfluentRegistryAvroRowSerializationSchema(schemaString, provider);
		final ConfluentRegistryAvroRowDeserializationSchema deserializationSchema =
				new ConfluentRegistryAvroRowDeserializationSchema(schemaString, provider);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testDeserializeSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final Schema schema = testData.f1.getSchema();
		final String schemaString = schema.toString();
		final SchemaCoder.SchemaCoderProvider provider = () -> new SchemaCoder() {
			@Override
			public Schema readSchema(InputStream in) {
				return schema;
			}

			@Override
			public void writeSchema(Schema schema, OutputStream out) throws IOException {
				//do nothing
			}
		};

		final ConfluentRegistryAvroRowSerializationSchema serializationSchema =
				new ConfluentRegistryAvroRowSerializationSchema(schemaString, provider);
		final ConfluentRegistryAvroRowDeserializationSchema deserializationSchema =
				new ConfluentRegistryAvroRowDeserializationSchema(schemaString, provider);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}
}
