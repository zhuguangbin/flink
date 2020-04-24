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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.CSRAvroValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of Avro-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class CSRAvroRowFormatFactory extends TableFormatFactoryBase<Row>
		implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	public CSRAvroRowFormatFactory() {
		super(CSRAvroValidator.FORMAT_TYPE_VALUE, 1, false);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(CSRAvroValidator.FORMAT_REGISTRY_URL);
		properties.add(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT);
		properties.add(CSRAvroValidator.FORMAT_RECORD_CLASS);
		properties.add(CSRAvroValidator.FORMAT_AVRO_SCHEMA);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		if (descriptorProperties.containsKey(CSRAvroValidator.FORMAT_RECORD_CLASS)) {
			return new CSRAvroRowDeserializationSchema(
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_URL),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT),
					descriptorProperties.getClass(CSRAvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class));
		} else {
			return new CSRAvroRowDeserializationSchema(
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_URL),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_AVRO_SCHEMA));
		}
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		if (descriptorProperties.containsKey(CSRAvroValidator.FORMAT_RECORD_CLASS)) {
			return new CSRAvroRowSerializationSchema(
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_URL),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT),
					descriptorProperties.getClass(CSRAvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class));
		} else {
			return new CSRAvroRowSerializationSchema(
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_URL),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT),
					descriptorProperties.getString(CSRAvroValidator.FORMAT_AVRO_SCHEMA));
		}
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new CSRAvroValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
