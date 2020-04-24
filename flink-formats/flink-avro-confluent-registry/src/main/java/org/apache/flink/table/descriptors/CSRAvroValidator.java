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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

/**
 * Validator for {@link CSRAvro}.
 */
@Internal
public class CSRAvroValidator extends FormatDescriptorValidator {

	public static final String FORMAT_TYPE_VALUE = "csravro";
	public static final String FORMAT_REGISTRY_URL = "format.registry-url";
	public static final String FORMAT_REGISTRY_SUBJECT = "format.registry-subject";
	public static final String FORMAT_RECORD_CLASS = "format.record-class";
	public static final String FORMAT_AVRO_SCHEMA = "format.avro-schema";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateString(FORMAT_REGISTRY_URL, false);
		properties.validateString(FORMAT_REGISTRY_SUBJECT, false);
		if (!properties.containsKey(FORMAT_REGISTRY_URL) || !properties.containsKey(FORMAT_REGISTRY_SUBJECT)) {
			throw new ValidationException("A defination of both registry-url and registry-subject is required.");
		}

		final boolean hasRecordClass = properties.containsKey(FORMAT_RECORD_CLASS);
		final boolean hasAvroSchema = properties.containsKey(FORMAT_AVRO_SCHEMA);
		if (hasRecordClass && hasAvroSchema) {
			throw new ValidationException("A definition of both a record-class and avro-schema is not allowed.");
		} else if (hasRecordClass) {
			properties.validateString(FORMAT_RECORD_CLASS, false, 1);
		} else if (hasAvroSchema) {
			properties.validateString(FORMAT_AVRO_SCHEMA, false, 1);
		} else {
			throw new ValidationException("A definition of record-class or avro-schema is required.");
		}
	}
}
