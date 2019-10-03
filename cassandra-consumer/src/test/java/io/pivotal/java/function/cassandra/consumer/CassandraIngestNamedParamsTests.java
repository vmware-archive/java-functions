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

package io.pivotal.java.function.cassandra.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.WriteResult;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.pivotal.java.function.cassandra.consumer.domain.Book;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Artem Bilan
 */
@TestPropertySource(properties = {
		"cassandra.cluster.init-script=init-db.cql",
		"cassandra.ingest-query=" +
				"insert into book (isbn, title, author, pages, saleDate, inStock) " +
				"values (:myIsbn, :myTitle, :myAuthor, ?, ?, ?)" })
class CassandraIngestNamedParamsTests extends CassandraConsumerApplicationTests {

	@Test
	void testIngestQuery() throws Exception {
		List<Book> books = getBookList(5);

		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		Jackson2JsonObjectMapper mapper = new Jackson2JsonObjectMapper(objectMapper);

		String booksJsonWithNamedParams = mapper.toJson(books);
		booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "isbn", "myIsbn");
		booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "title", "myTitle");
		booksJsonWithNamedParams = StringUtils.replace(booksJsonWithNamedParams, "author", "myAuthor");

		Mono<? extends WriteResult> result =
				this.cassandraConsumer.apply(booksJsonWithNamedParams);

		StepVerifier.create(result)
				.expectNextCount(1)
				.then(() ->
						assertThat(this.cassandraTemplate.query(Book.class)
								.count())
								.isEqualTo(5))
				.verifyComplete();
	}

}
