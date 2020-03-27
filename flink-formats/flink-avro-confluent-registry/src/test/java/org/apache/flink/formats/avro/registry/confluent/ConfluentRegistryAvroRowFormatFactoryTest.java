/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.SchemaRegistryAvro;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link ConfluentRegistryAvroRowFormatFactory}.
 */
public class ConfluentRegistryAvroRowFormatFactoryTest {

	private static final String AVRO_SCHEMA = "{\"type\":\"record\"," +
			"\"name\":\"Payment\"," +
			"\"namespace\":\"io.confluent.examples.clients.basicavro\"," +
			"\"fields\":[{\"name\":\"id\",\"type\":\"string\"}," +
			"{\"name\":\"amount\",\"type\":\"double\"}]}";

	private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	private static final String SUBJECT = "subject1";

	@Test
	public void testConfluentRegistryAvroRowDeserializationSchema() {
		final Map<String, String> properties = getProperties();
		final DeserializationSchema<?> actual = TableFactoryService
				.find(DeserializationSchemaFactory.class, properties)
				.createDeserializationSchema(properties);
		final ConfluentRegistryAvroRowDeserializationSchema expected =
				new ConfluentRegistryAvroRowDeserializationSchema(SCHEMA_REGISTRY_URL, AVRO_SCHEMA);
		assertEquals(expected, actual);
	}

	@Test
	public void testConfluentRegistryAvroRowSerializationSchema() {
		final Map<String, String> properties = getProperties();
		final SerializationSchema<?> actual = TableFactoryService
				.find(SerializationSchemaFactory.class, properties)
				.createSerializationSchema(properties);
		final ConfluentRegistryAvroRowSerializationSchema expected =
				new ConfluentRegistryAvroRowSerializationSchema(SCHEMA_REGISTRY_URL,
						SUBJECT, AVRO_SCHEMA);
		assertEquals(expected, actual);
	}

	//~ Tools ------------------------------------------------------------------

	private static Map<String, String> getProperties() {
		return new SchemaRegistryAvro()
				.schemaRegistryURL(SCHEMA_REGISTRY_URL)
				.avroSchema(AVRO_SCHEMA)
				.subject(SUBJECT)
				.toProperties();
	}
}
