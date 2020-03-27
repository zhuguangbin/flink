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

package org.apache.flink.catalog.confluent;

import org.apache.flink.catalog.confluent.util.SchemaRegistryService;
import org.apache.flink.catalog.confluent.util.Util;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Unit test for {@link SchemaRegistryService}. */
public class SchemaRegistryServiceTest {
	private SchemaRegistryService service;

	@Before
	public void before() {
		this.service = new MockSchemaRegistryService("http://localhost:8081");
	}

	@Test
	public void testAvroSchema2TableSchema() throws IOException {
		String result = service.getLatestSchemaForTopic("topic1");
		String expected = "root\n" +
			" |-- id: STRING\n" +
			" |-- amount: DOUBLE\n";
		assertThat(Util.avroSchema2TableSchema(result).toString(), is(expected));
	}
}
