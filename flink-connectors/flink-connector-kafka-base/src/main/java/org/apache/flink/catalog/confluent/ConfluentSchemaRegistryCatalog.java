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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.catalog.confluent.util.SchemaRegistryService;
import org.apache.flink.catalog.confluent.util.Util;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.AvroValidator;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaRegistryAvroValidator;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.util.Preconditions;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Catalog for
 * <a href="https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html">Confluent Schema Registry</a>.
 * It allows to access all the topics of current Confluent Schema Registry Service
 * through SQL or TableAPI, there is no need to create any table explicitly.
 *
 * <p>The code snippet below illustrates how to use this catalog:
 * <pre>
 *     ConfluentSchemaRegistryCatalog catalog = new ConfluentSchemaRegistryCatalog(
 * 			properties,
 * 			schemaRegistryURL,
 * 			"catalog1",
 * 			"db1");
 * 		tEnv.registerCatalog("catalog1", catalog);
 *
 * 		// ---------- Consume stream from Kafka -------------------
 *
 * 		String query = "SELECT\n" +
 * 			"  id, amount\n" +
 * 			"FROM catalog1.db1.transactions1";
 * </pre>
 *
 * <p>We only support TopicNameStrategy for subject naming strategy,
 * for which all the records in one topic has the same schema, see
 * <a href="https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work">How the Naming Strategies Work</a>
 * for details.
 *
 * <p>You can specify some common options for these topics. All the tables from this catalog
 * would take the same options. If this is not your request, use dynamic table options set up
 * within per-table scope.
 */
public class ConfluentSchemaRegistryCatalog extends AbstractCatalog {
	private static final String CONNECTOR_PROPS_ERROR_FORMAT =
		"No need to specify property '%s' for "
			+ ConfluentSchemaRegistryCatalog.class.getSimpleName();

	/** Service to interact with the Confluent Schema Registry. */
	private SchemaRegistryService service;

	/** All topic name list snapshot of the schema registry when this catalog is open. */
	private List<String> allTopics = Collections.emptyList();

	// Single database for this catalog with default name.
	private CatalogDatabase database;

	/** Additional connector properties for all the Kafka topics. */
	private ImmutableMap<String, String> connectorProps;

	private LoadingCache<Key, CatalogBaseTable> tableCache;

	public ConfluentSchemaRegistryCatalog(
			Map<String, String> connectorProps,
			String schemaRegistryURL,
			String name,
			String defaultDatabase) {
		super(name, defaultDatabase);
		// validate the connector properties are valid.
		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(Objects.requireNonNull(connectorProps));
		Set<String> forbiddenConnectorProps = Sets.newHashSet(
			ConnectorDescriptorValidator.CONNECTOR_TYPE,
			FormatDescriptorValidator.FORMAT_TYPE
		);
		for (String forbidden : forbiddenConnectorProps) {
			if (descriptorProperties.containsKey(forbidden)) {
				throw new ValidationException(String.format(CONNECTOR_PROPS_ERROR_FORMAT,
					AvroValidator.FORMAT_RECORD_CLASS));
			}
		}

		Map<String, String> overriddenProps = new HashMap<>(connectorProps);
		overriddenProps.put(ConnectorDescriptorValidator.CONNECTOR_TYPE,
			KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA);
		this.connectorProps = ImmutableMap.copyOf(overriddenProps);

		this.service = new SchemaRegistryService(schemaRegistryURL);
		this.database = new CatalogDatabaseImpl(Collections.emptyMap(),
			String.format("ConfluentSchemaRegistryCatalog data base: %s", defaultDatabase));
		this.tableCache = CacheBuilder.newBuilder()
			.maximumSize(1000)
			.softValues()
			.build(CacheLoader.from(ConfluentSchemaRegistryCatalog::generatesCatalogTable));
	}

	@Override
	public void open() throws CatalogException {
		try {
			this.allTopics = service.getAllSubjectTopics();
		} catch (IOException e) {
			throw new CatalogException("Error when fetching topic name list", e);
		}
	}

	@Override
	public void close() throws CatalogException {
		this.allTopics.clear();
		this.allTopics = null;
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		return Collections.singletonList(getDefaultDatabase());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Preconditions.checkNotNull(databaseName);
		if (!databaseName.equals(getDefaultDatabase())) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
		return database;
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return Objects.equals(databaseName, getDefaultDatabase());
	}

	@Override
	public List<String> listTables(String databaseName) throws CatalogException {
		return this.allTopics;
	}

	@Override
	public List<String> listViews(String databaseName) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		} else {
			Key key = Key.from(service, tablePath.getObjectName(), this.connectorProps);
			CatalogBaseTable catalogBaseTable = this.tableCache.getIfPresent(key);
			if (catalogBaseTable != null) {
				return catalogBaseTable;
			}
			return this.tableCache.getUnchecked(key);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		String dbName = tablePath.getDatabaseName();
		return databaseExists(dbName) && allTopics.contains(tablePath.getObjectName());
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws CatalogException {
		// Can be supported in the near future.
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listFunctions(String dbName) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		return CatalogTableStatistics.UNKNOWN;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		return CatalogColumnStatistics.UNKNOWN;
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@VisibleForTesting
	public void setService(SchemaRegistryService service) {
		this.service = service;
	}

	//~ Tools ------------------------------------------------------------------

	private static CatalogBaseTable generatesCatalogTable(Key key) {
		try {
			String avroSchema = key.service.getLatestSchemaForTopic(key.topicName);
			TableSchema tableSchema = Util.avroSchema2TableSchema(avroSchema);
			Map<String, String> connectorProps = new HashMap<>(key.connectorProps);
			connectorProps.put(KafkaValidator.CONNECTOR_TOPIC, key.topicName);
			connectorProps.put(SchemaRegistryAvroValidator.FORMAT_SCHEMA_REGISTRY_URL, key.service.schemaRegistryURL);
			connectorProps.put(SchemaRegistryAvroValidator.FORMAT_AVRO_SCHEMA, avroSchema);
			connectorProps.put(FormatDescriptorValidator.FORMAT_TYPE, SchemaRegistryAvroValidator.FORMAT_TYPE_VALUE);
			final String subject = String.format("%s-value", key.topicName);
			connectorProps.put(SchemaRegistryAvroValidator.FORMAT_SCHEMA_REGISTRY_SUBJECT, subject);

			return new CatalogTableImpl(
				tableSchema,
				connectorProps,
				"Schema Registry table for topic: " + key.topicName);
		} catch (IOException e) {
			throw new CatalogException("Error while fetching schema info "
				+ "from Confluent Schema Registry", e);
		}
	}

	//~ Inner Class ------------------------------------------------------------

	/** Table cache key. */
	private static class Key {
		private final SchemaRegistryService service;
		private final String topicName;
		private final Map<String, String> connectorProps;
		Key(SchemaRegistryService service, String topicName, Map<String, String> connectorProps) {
			this.service = service;
			this.topicName = topicName;
			this.connectorProps = connectorProps;
		}

		public static Key from(
				SchemaRegistryService service,
				String topicName,
				Map<String, String> connectorProps) {
			return new Key(service, topicName, connectorProps);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Key key = (Key) o;
			return Objects.equals(topicName, key.topicName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(topicName);
		}
	}
}
