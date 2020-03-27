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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

/**
 * Validator for {@link SchemaRegistryAvro}.
 */
@Internal
public class SchemaRegistryAvroValidator extends FormatDescriptorValidator {

	// Confluent schema registry.
	public static final String FORMAT_TYPE_VALUE = "avro-confluent";
	public static final String FORMAT_SCHEMA_REGISTRY_URL = "format.schema-registry.url";
	public static final String FORMAT_SCHEMA_REGISTRY_SUBJECT = "format.schema-registry.subject";
	public static final String FORMAT_AVRO_SCHEMA = "format.avro-schema";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		final boolean hasSchemaRegistryUrl = properties.containsKey(FORMAT_SCHEMA_REGISTRY_URL);
		final boolean hasAvroSchema = properties.containsKey(FORMAT_AVRO_SCHEMA);
		if (hasSchemaRegistryUrl) {
			properties.validateString(FORMAT_SCHEMA_REGISTRY_URL, false, 1);
		} else {
			throw new ValidationException("A definition of "
					+ "Confluent schema registry url "
					+ "is required.");
		}
		if (hasAvroSchema) {
			properties.validateString(FORMAT_AVRO_SCHEMA, false, 1);
		} else {
			throw new ValidationException("A definition of Avro schema is required.");
		}
	}
}
