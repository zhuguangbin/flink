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

import java.util.ArrayList;
import java.util.List;

/** A mock schema registry service for test. */
public class MockSchemaRegistryService extends SchemaRegistryService {
	public static final String SCHEMA = "{\"type\":\"record\"," +
			"\"name\":\"Payment\"," +
			"\"namespace\":\"io.confluent.examples.clients.basicavro\"," +
			"\"fields\":[{\"name\":\"id\",\"type\":\"string\"}," +
			"{\"name\":\"amount\",\"type\":\"double\"}]}";

	MockSchemaRegistryService(String url) {
		super(url);
	}

	@Override
	public List<String> getAllSubjectTopics() {
		List<String> topics = new ArrayList<>();
		topics.add("topic1");
		topics.add("topic2");
		topics.add("topic3");
		return topics;
	}

	@Override
	public String getLatestSchemaForTopic(String topic) {
		return SCHEMA;
	}
}
