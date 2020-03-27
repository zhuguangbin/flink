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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Test casts for {@link ConfluentSchemaRegistryCatalog}. */
public class ConfluentSchemaRegistryCatalogTest {
	private static final String SCHEMA_REGISTRY_URL = "url";
	private static final String DB_NAME = "db1";

	private ConfluentSchemaRegistryCatalog catalog;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	@Before
	public void before() {
		this.catalog = new ConfluentSchemaRegistryCatalog(
				Collections.emptyMap(),
				SCHEMA_REGISTRY_URL,
				"confluent",
				DB_NAME);
		this.catalog.setService(new MockSchemaRegistryService(SCHEMA_REGISTRY_URL));
		this.catalog.open();
	}

	@After
	public void after() {
		if (this.catalog != null) {
			this.catalog.close();
			this.catalog = null;
		}
	}

	@Test
	public void testListDatabases() {
		List<String> dbs = this.catalog.listDatabases();
		assertThat(dbs, is(Collections.singletonList(DB_NAME)));
	}

	@Test
	public void testGetDatabase() throws DatabaseNotExistException {
		CatalogDatabase db = this.catalog.getDatabase(DB_NAME);
		assertThat(db.getProperties(), is(Collections.emptyMap()));
		assertThat(db.getComment(),
				is(String.format("ConfluentSchemaRegistryCatalog data base: %s", DB_NAME)));
		exceptionRule.expect(DatabaseNotExistException.class);
		this.catalog.getDatabase("db2");
	}

	@Test
	public void testGetTable() throws TableNotExistException {
		CatalogBaseTable table = this.catalog.getTable(ObjectPath.fromString(DB_NAME + ".topic1"));
		Map<String, String> props = table.getProperties();
		Map<String, String> expectedProps = new HashMap<>();
		expectedProps.put("connector.type", "kafka");
		expectedProps.put("format.schema-registry.subject", "topic1-value");
		expectedProps.put("format.schema-registry.url", "url");
		expectedProps.put("connector.topic", "topic1");
		expectedProps.put("format.avro-schema", MockSchemaRegistryService.SCHEMA);
		expectedProps.put("format.type", "avro-confluent");
		assertThat(props, is(expectedProps));
		TableSchema schema = table.getSchema();
		String expectedSchema = "root\n"
				+ " |-- id: STRING\n"
				+ " |-- amount: DOUBLE\n";
		assertThat(schema.toString(), is(expectedSchema));
	}

	@Test
	public void testListTables() throws DatabaseNotExistException {
		List<String> tables = this.catalog.listTables(DB_NAME);
		assertThat(tables, is(Arrays.asList("topic1", "topic2", "topic3")));
	}

	@Test
	public void testTableExists() {
		assertThat(this.catalog.tableExists(ObjectPath.fromString(DB_NAME + ".topic1")), is(true));
		assertThat(this.catalog.tableExists(ObjectPath.fromString(DB_NAME + ".topic4")), is(false));
	}
}
