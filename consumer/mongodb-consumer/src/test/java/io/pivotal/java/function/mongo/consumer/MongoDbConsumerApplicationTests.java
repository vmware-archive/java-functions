/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.CountDownLatch;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author David Turanski
 */
@SpringBootTest(properties = {
		"mongodb.collection=testing"})
class MongoDbConsumerApplicationTests {

	@Autowired
	private MongoDbConsumerProperties properties;

	@Autowired
	private MongoDbConsumerConfiguration.MongoDbConsumer mongoDbConsumer;

	@Autowired
	private ReactiveMongoTemplate mongoTemplate;

	@Test
	void testMongodbConsumer() throws InterruptedException {
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

		CountDownLatch latch = new CountDownLatch(3);

		mongoDbConsumer.setSubscriber(new Subscriber<Void>() {
			@Override
			public void onSubscribe(Subscription subscription) {
			}

			@Override
			public void onNext(Void aVoid) {
			}

			@Override
			public void onError(Throwable throwable) {
			}

			@Override
			public void onComplete() {
				latch.countDown();
			}
		});

		mongoDbConsumer.accept(messages);
		latch.await();

		StepVerifier.create(this.mongoTemplate.findAll(Document.class, properties.getCollection())
				.sort(Comparator.comparing(d -> d.get("_id").toString())))
				.assertNext(document -> {
					assertThat(document.get("foo")).isEqualTo("bar");
				})
				.assertNext(document-> {
					assertThat(document.get("firstName")).isEqualTo("Foo");
					assertThat(document.get("lastName")).isEqualTo("Bar");
				})
				.assertNext(document-> {
					assertThat(document.get("my_data")).isEqualTo("THE DATA");
				})
				.verifyComplete();
	}
}
