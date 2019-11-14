/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.mongo.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 */
@SpringBootTest(properties = {
		"mongodb.collection=testing"})
class MongodbConsumerApplicationTests {

	@Autowired
	private MongodbConsumerProperties properties;

	@Autowired
	private MongodbConsumerConfiguration.MongodbConsumer mongodbConsumer;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Test
	void testMongodbConsumer() {
			Map<String, String> data1 = new HashMap<>();
			data1.put("foo", "bar");

			Map<String, String> data2 = new HashMap<>();
			data2.put("firstName", "Foo");
			data2.put("lastName", "Bar");

			Flux<Message<?>> messages = Flux.just(
					new GenericMessage<>(data1),
					new GenericMessage<>(data2),
					new GenericMessage<>("{\"my_data\": \"THE DATA\"}")
			);

			mongodbConsumer.accept(messages);

			List<Document> result =
					this.mongoTemplate.findAll(Document.class, properties.getCollection());

			assertThat(result.size()).isEqualTo(3);

			Document dbObject = result.get(0);
			assertThat(dbObject.get("_id")).isNotNull();
			assertThat(dbObject.get("foo")).isEqualTo("bar");

			dbObject = result.get(1);
			assertThat(dbObject.get("_id")).isNotNull();
			assertThat(dbObject.get("firstName")).isEqualTo("Foo");
			assertThat(dbObject.get("lastName")).isEqualTo("Bar");

			dbObject = result.get(2);
			assertThat(dbObject.get("_id")).isNotNull();
			assertThat(dbObject.get("my_data")).isEqualTo("THE DATA");
	}
}
