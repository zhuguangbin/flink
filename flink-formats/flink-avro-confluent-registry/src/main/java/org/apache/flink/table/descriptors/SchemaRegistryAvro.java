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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Format descriptor for Apache Avro records from Confluent Schema Registry.
 */
@PublicEvolving
public class SchemaRegistryAvro extends FormatDescriptor {
	private String schemaRegistryURL;
	private String subject;
	private String avroSchema;

	/**
	 * Format descriptor for Apache Avro records.
	 */
	public SchemaRegistryAvro() {
		super(SchemaRegistryAvroValidator.FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets the URL of Confluent Schema Registry.
	 *
	 * @param schemaRegistryURL Confluent Schema Registry URL
	 */
	public SchemaRegistryAvro schemaRegistryURL(String schemaRegistryURL) {
		Preconditions.checkNotNull(schemaRegistryURL);
		this.schemaRegistryURL = schemaRegistryURL;
		return this;
	}

	/** Sets the subject to write sink data. */
	public SchemaRegistryAvro subject(String subject) {
		Preconditions.checkNotNull(subject);
		this.subject = subject;
		return this;
	}

	/**
	 * Sets the Avro schema for specific or generic Avro records.
	 *
	 * @param avroSchema Avro schema string
	 */
	public SchemaRegistryAvro avroSchema(String avroSchema) {
		Preconditions.checkNotNull(avroSchema);
		this.avroSchema = avroSchema;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (null != schemaRegistryURL) {
			properties.putString(SchemaRegistryAvroValidator.FORMAT_SCHEMA_REGISTRY_URL,
					schemaRegistryURL);
		}

		if (null != subject) {
			properties.putString(SchemaRegistryAvroValidator.FORMAT_SCHEMA_REGISTRY_SUBJECT,
					subject);
		}

		if (null != avroSchema) {
			properties.putString(SchemaRegistryAvroValidator.FORMAT_AVRO_SCHEMA,
					avroSchema);
		}

		return properties.asMap();
	}
}
