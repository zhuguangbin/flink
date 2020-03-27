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

package org.apache.flink.catalog.confluent.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.TableSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;

/** Utilities. */
public abstract class Util {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static ObjectMapper getObjectMapper() {
		return OBJECT_MAPPER;
	}

	/** Gets the HTTP result as a string. The 'Accept' property is set to 'application/json'. */
	public static String getHttpResponse(String urlStr) throws IOException {
		URL url = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		conn.setConnectTimeout(5000);

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
				+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(
			(conn.getInputStream())));

		StringBuilder outputBuilder = new StringBuilder();
		String output;
		while ((output = br.readLine()) != null) {
			outputBuilder.append(output);
		}
		conn.disconnect();
		return outputBuilder.toString();
	}

	/** Strips out the tailing '/' of the input string. */
	public static String stripTailingSlash(String schemaRegistryURL) {
		Objects.requireNonNull(schemaRegistryURL);
		int idx = schemaRegistryURL.length() - 1;
		if (idx == 0) {
			return schemaRegistryURL;
		}
		while (idx > 0 && schemaRegistryURL.charAt(idx) == '/') {
			idx--;
		}
		return schemaRegistryURL.substring(0, idx + 1);
	}

	/**
	 * Build {@link TableSchema} with given Avro schema string.
	 *
	 * <p>For example,
	 * <pre>
	 *     {
	 *         "type":"record",
	 *         "name":"Payment",
	 *         "namespace":"io.confluent.examples.clients.basicavro",
	 *         "fields":[
	 *             {"name":"id","type":"string"},
	 *             {"name":"amount","type":"double"}
	 *             ]
	 *     }
	 * </pre>
	 * would build a table schema (id: STRING, amount: DOUBLE).
	 *
	 * @param avroSchema Avro schema of json string format
	 *
	 * @return TableSchema of logical table
	 */
	public static TableSchema avroSchema2TableSchema(String avroSchema) {
		TypeInformation<?> typeInfo = AvroSchemaConverter.convertToTypeInfo(avroSchema);
		return TableSchema.fromTypeInfo(typeInfo);
	}
}
