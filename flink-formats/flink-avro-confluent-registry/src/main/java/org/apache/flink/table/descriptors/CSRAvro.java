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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

/**
 * Format descriptor for Apache CSRAvro records.
 */
@PublicEvolving
public class CSRAvro extends FormatDescriptor {

	private String registryUrl;
	private String registrySubject;
	private Class<? extends SpecificRecord> recordClass;
	private String avroSchema;

	/**
	 * Format descriptor for Apache CSRAvro records.
	 */
	public CSRAvro() {
		super(CSRAvroValidator.FORMAT_TYPE_VALUE, 1);
	}


	/**
	 * Set schema registry url.
	 *
	 * @param registryUrl schema registry url.
	 */
	public CSRAvro registryUrl(String registryUrl) {
		Preconditions.checkNotNull(registryUrl);
		this.registryUrl = registryUrl;
		return this;
	}

	/**
	 * Registry subject of schema.
	 * @param registrySubject subject name of schema.
	 * @return
	 */
	public CSRAvro registrySubject(String registrySubject) {
		Preconditions.checkNotNull(registrySubject);
		this.registrySubject = registrySubject;
		return this;
	}

	/**
	 * Sets the class of the CSRAvro specific record.
	 *
	 * @param recordClass class of the CSRAvro record.
	 */
	public CSRAvro recordClass(Class<? extends SpecificRecord> recordClass) {
		Preconditions.checkNotNull(recordClass);
		this.recordClass = recordClass;
		return this;
	}

	/**
	 * Sets the CSRAvro schema for specific or generic CSRAvro records.
	 *
	 * @param avroSchema CSRAvro schema string.
	 */
	public CSRAvro avroSchema(String avroSchema) {
		Preconditions.checkNotNull(avroSchema);
		this.avroSchema = avroSchema;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (null != registryUrl) {
			properties.putString(CSRAvroValidator.FORMAT_REGISTRY_URL, registryUrl);
		}
		if (null != registrySubject) {
			properties.putString(CSRAvroValidator.FORMAT_REGISTRY_SUBJECT, registrySubject);
		}
		if (null != recordClass) {
			properties.putClass(CSRAvroValidator.FORMAT_RECORD_CLASS, recordClass);
		}
		if (null != avroSchema) {
			properties.putString(CSRAvroValidator.FORMAT_AVRO_SCHEMA, avroSchema);
		}

		return properties.asMap();
	}
}
