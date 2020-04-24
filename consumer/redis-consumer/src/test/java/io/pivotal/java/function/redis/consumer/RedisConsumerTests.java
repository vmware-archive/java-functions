/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.redis.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Pollack
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Soby Chacko
 */
@SpringBootTest(properties = {"spring.redis.host=${embedded.redis.host}",
							"spring.redis.port=${embedded.redis.port}",
							"spring.redis.user=${embedded.redis.user}",
							"spring.redis.password=${embedded.redis.password}",
							"redis.consumer.key=foo"})
@DirtiesContext
public class RedisConsumerTests {

	@Autowired
	Consumer<Message<?>> redisConsumer;

	@Autowired
	private RedisConnectionFactory redisConnectionFactory;

	@Test
	public void testWithKey() throws Exception{
		//Setup
		String key = "foo";
		StringRedisTemplate redisTemplate = createStringRedisTemplate(redisConnectionFactory);
		redisTemplate.delete(key);

		RedisList<String> redisList = new DefaultRedisList<>(key, redisTemplate);
		List<String> list = new ArrayList<>();
		list.add("Manny");
		list.add("Moe");
		list.add("Jack");

		//Execute
		Message<List<String>> message = new GenericMessage<>(list);

		redisConsumer.accept(message);

		//Assert
		assertThat(redisList.size()).isEqualTo(3);
		assertThat(redisList.get(0)).isEqualTo("Manny");
		assertThat(redisList.get(1)).isEqualTo("Moe");
		assertThat(redisList.get(2)).isEqualTo("Jack");

		//Cleanup
		redisTemplate.delete(key);
	}

	StringRedisTemplate createStringRedisTemplate(RedisConnectionFactory connectionFactory) {
		StringRedisTemplate redisTemplate = new StringRedisTemplate();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}

	@SpringBootApplication
	static class TestApplication {

	}
}
