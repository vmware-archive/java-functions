/*
 * Copyright (c) 2011-2020 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.transformer.function;

import java.util.function.Function;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.transformer.ExpressionEvaluatingTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

@Configuration
@EnableConfigurationProperties(TransformFunctionProperties.class)
public class TransformFunctionConfiguration {

	@Bean
	public Function<Message<?>, Message<?>> transformFunction(TransformFunctionProperties transformFunctionProperties) {
			return message -> {
				if (message.getPayload() instanceof byte[]) {
					final MessageHeaders headers = message.getHeaders();
					String contentType =
							headers.containsKey(MessageHeaders.CONTENT_TYPE)
									? headers.get(MessageHeaders.CONTENT_TYPE).toString()
									: "application/json";
					if (contentType.contains("text") || contentType.contains("json") || contentType.contains("x-spring-tuple")) {
						message = MessageBuilder.withPayload(new String(((byte[]) message.getPayload())))
								.copyHeaders(message.getHeaders())
								.build();
					}
				}
				final ExpressionEvaluatingTransformer expressionEvaluatingTransformer = new ExpressionEvaluatingTransformer(
						new SpelExpressionParser()
								.parseExpression(transformFunctionProperties.getExpression()));
				return expressionEvaluatingTransformer.transform(message);
			};
	}
}
