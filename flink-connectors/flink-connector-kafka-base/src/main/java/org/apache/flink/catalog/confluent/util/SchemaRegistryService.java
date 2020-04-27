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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Schema Registry Service to interact with Confluent Schema Registry. */
public class SchemaRegistryService {
	private static final String URL_PATTERN_ALL_SUBJECTS = "%s/subjects/";
	private static final String URL_PATTERN_LATEST_SUBJECT =
		"%s/subjects/%s-value/versions/latest";

	/**
	 * We only support TopicNameStrategy for subject naming strategy,
	 * for which all the records in one topic has the same schema, see
	 * <a href="https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work">How the Naming Strategies Work</a>
	 * for details.
	 * */
	private static final String TOPIC_NAME_STRATEGY_PREFIX = "-value";

	/** Confluent schema registry URL. */
	public final String schemaRegistryURL;

	/** URL to fetch all the subjects of current schema registry service. */
	private String fetchAllSubjectsURL;

	/**
	 * Creates a {@code SchemaRegistryService}.
	 *
	 * @param schemaRegistryURL The Confluent Schema Registry URL
	 */
	public SchemaRegistryService(String schemaRegistryURL) {
		this.schemaRegistryURL = Util.stripTailingSlash(schemaRegistryURL);
		this.fetchAllSubjectsURL = String.format(URL_PATTERN_ALL_SUBJECTS, schemaRegistryURL);
	}

	/** Returns all the topic list from the schema registry service of current cluster. */
	@SuppressWarnings("unchecked")
	public List<String> getAllSubjectTopics() throws IOException {
		String result = Util.getHttpResponse(this.fetchAllSubjectsURL);
		List<String> allSubjects = Util.getObjectMapper().readValue(result, List.class);
		return allSubjects.stream()
			.filter(sub -> sub.endsWith(TOPIC_NAME_STRATEGY_PREFIX))
			// Strips out the "-value" suffix.
			.map(sub -> sub.substring(0, sub.length() - 6))
			.collect(Collectors.toList());
	}

	/** Returns the Avro schema string with given topic name. */
	@SuppressWarnings("unchecked")
	public String getLatestSchemaForTopic(String topic) throws IOException {
		String urlStr = String.format(URL_PATTERN_LATEST_SUBJECT, this.schemaRegistryURL, topic);
		String result = Util.getHttpResponse(urlStr);
		Map<String, String> subjectInfo = Util.getObjectMapper().readValue(result, Map.class);
		if (subjectInfo != null) {
			return subjectInfo.get("schema");
		} else {
			return null;
		}
	}
}
