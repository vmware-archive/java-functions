/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.function.Consumer;

import reactor.core.publisher.Flux;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.mongodb.outbound.MongoDbStoringMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;

/**
 * A configuration for MongoDB Consumer function. Uses a {@link MongoDbStoringMessageHandler}
 * to save payload contents to Mongo DB.
 *
 * @author Artem Bilan
 * @author David Turanski
 *
 */
@Configuration
@EnableConfigurationProperties({ MongodbConsumerProperties.class })
public class MongodbConsumerConfiguration {

	private final MongodbConsumerProperties properties;

	private final MongoTemplate mongoTemplate;

	public MongodbConsumerConfiguration(MongodbConsumerProperties properties, MongoTemplate mongoTemplate) {
		this.properties = properties;
		this.mongoTemplate = mongoTemplate;
	}

	@Bean
	public MongodbConsumer mongodbConsumer(MessageHandler mongoConsumerMessageHandler) {
		return (Flux<Message<?>> flux) -> flux.subscribe(msg -> mongoConsumerMessageHandler.handleMessage(msg));
	}

	@Bean
	public MessageHandler mongoConsumerMessageHandler() {
		MongoDbStoringMessageHandler mongoDbMessageHandler = new MongoDbStoringMessageHandler(this.mongoTemplate);
		Expression collectionExpression = this.properties.getCollectionExpression();
		if (collectionExpression == null) {
			collectionExpression = new LiteralExpression(this.properties.getCollection());
		}
		mongoDbMessageHandler.setCollectionNameExpression(collectionExpression);
		return mongoDbMessageHandler;
	}

	interface MongodbConsumer extends Consumer<Flux<Message<?>>> {
	}

}
