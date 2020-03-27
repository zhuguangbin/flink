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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.util.Objects;

/** Confluent registry Avro row deserialization schema. */
public class ConfluentRegistryAvroRowDeserializationSchema
		extends AvroRowDeserializationSchema {

	/** Provider for schema coder. Used for initializing in each task. */
	private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	/** Coder used for reading schema from incoming stream. */
	private transient SchemaCoder schemaCoder;

	public ConfluentRegistryAvroRowDeserializationSchema(
			String schemaRegistryURL,
			String avroSchemaString) {
		this(avroSchemaString, new CachedSchemaCoderProvider(schemaRegistryURL));
	}

	@VisibleForTesting
	public ConfluentRegistryAvroRowDeserializationSchema(
			String avroSchemaString,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(avroSchemaString);
		// Provider for schema coder. Used for initializing in each task.
		this.schemaCoderProvider = schemaCoderProvider;
	}

	@Override
	protected void preRecordRead(
			DatumReader<IndexedRecord> reader,
			MutableByteArrayInputStream inputStream) {
		if (schemaCoder == null) {
			this.schemaCoder = schemaCoderProvider.get();
		}
		try {
			Schema writerSchema = schemaCoder.readSchema(inputStream);
			reader.setSchema(writerSchema);
		} catch (IOException e) {
			throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		ConfluentRegistryAvroRowDeserializationSchema that =
				(ConfluentRegistryAvroRowDeserializationSchema) o;
		return schemaCoderProvider.equals(that.schemaCoderProvider);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), schemaCoderProvider);
	}
}
