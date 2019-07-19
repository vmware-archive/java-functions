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
package io.pivotal.java.function.jdbc.supplier;

import java.util.function.Function;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;

@Configuration
@EnableConfigurationProperties(JdbcSupplierProperties.class)
public class JdbcSupplierConfiguration {

	@Autowired
	JdbcSupplierProperties properties;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private Function<Message<?>, Flux<Message<?>>> splitterFunction;

	@Bean
	public MessageSource<Object> jdbcMessageSource() {
		JdbcPollingChannelAdapter jdbcPollingChannelAdapter =
				new JdbcPollingChannelAdapter(this.dataSource, this.properties.getQuery());
		jdbcPollingChannelAdapter.setMaxRowsPerPoll(this.properties.getMaxRowsPerPoll());
		jdbcPollingChannelAdapter.setUpdateSql(this.properties.getUpdate());
		return jdbcPollingChannelAdapter;
	}

	@Bean
	public Supplier<Message<?>> get() {
		return () -> {
			final Message<?> received = jdbcMessageSource().receive();
			System.out.println("Data received from JDBC Source: " + received);
			if (properties.isSplit()) {
				splitterFunction.apply(received);
			}
			return received;
		};
	}
}
