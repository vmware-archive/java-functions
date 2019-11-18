package io.pivotal.java.function.rabbit.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RabbitConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RabbitConsumerApplication.class, args);
	}
}
