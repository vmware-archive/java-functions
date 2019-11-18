package io.pivotal.java.function.splitter.function;

import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public abstract class SplitterFunctionApplicationTests {

	@Autowired
	protected Function<Message<?>, Flux<Message<?>>> splitter;

	@TestPropertySource(properties = "splitter.expression=payload.split(',')")
	public static class UsingExpressionIntegrationTests extends SplitterFunctionApplicationTests {

		@Test
		public void test() {
			Flux<Message<?>> messageFlux = this.splitter.apply(new GenericMessage<>("hello,world"));

			StepVerifier.create(
					messageFlux
							.map(Message::getPayload)
							.cast(String.class))
					.expectNext("hello", "world")
					.verifyComplete();
		}

	}

}
