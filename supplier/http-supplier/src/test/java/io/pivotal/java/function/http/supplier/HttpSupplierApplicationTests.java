package io.pivotal.java.function.http.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * The test for HTTP Supplier.
 *
 * @author Artem Bilan
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HttpSupplierApplicationTests {

	@Autowired
	private TestRestTemplate testRestTemplate;

	@Autowired
	private Supplier<Flux<Message<byte[]>>> httpSupplier;

	@Test
	public void testHttpSupplier() {
		Flux<Message<byte[]>> messageFlux = this.httpSupplier.get();

		StepVerifier stepVerifier =
				StepVerifier.create(messageFlux)
						.assertNext((message) ->
								assertThat(message)
										.satisfies((msg) -> assertThat(msg)
												.extracting(Message::getPayload)
												.isEqualTo("test1".getBytes()))
										.satisfies((msg) -> assertThat(msg.getHeaders())
												.containsEntry(MessageHeaders.CONTENT_TYPE,
														new MediaType("text", "plain", StandardCharsets.ISO_8859_1)))
						)
						.assertNext((message) ->
								assertThat(message)
										.extracting(Message::getPayload)
										.isEqualTo("{\"name\":\"test2\"}".getBytes()))
						.assertNext((message) ->
								assertThat(message)
										.extracting(Message::getPayload)
										.isEqualTo("{\"name\":\"test3\"}".getBytes()))
						.thenCancel()
						.verifyLater();

		this.testRestTemplate.postForObject("/", "test1", void.class);
		this.testRestTemplate.postForObject("/", new TestPojo("test2"), void.class);
		this.testRestTemplate.postForObject("/", new TestPojo("test3"), void.class);

		stepVerifier.verify();
	}

	private static class TestPojo {

		private String name;

		public TestPojo() {
		}

		public TestPojo(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

}
