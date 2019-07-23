package io.pivotal.java.function.rabbit.consumer;

import javax.validation.constraints.AssertTrue;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.expression.Expression;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("rabbit")
@Validated
public class RabbitConsumerProperties {

	public static final String JSON_CONVERTER = "jsonConverter";

	/**
	 * Exchange name - overridden by exchangeNameExpression, if supplied.
	 */
	private String exchange = "";

	/**
	 * A SpEL expression that evaluates to an exchange name.
	 */
	private Expression exchangeExpression;

	/**
	 * Routing key - overridden by routingKeyExpression, if supplied.
	 */
	private String routingKey;

	/**
	 * A SpEL expression that evaluates to a routing key.
	 */
	private Expression routingKeyExpression;

	/**
	 * Default delivery mode when 'amqp_deliveryMode' header is not present,
	 * true for PERSISTENT.
	 */
	private boolean persistentDeliveryMode;

	/**
	 * Headers that will be mapped.
	 */
	private String[] mappedRequestHeaders = { "*" };

	/**
	 * The bean name for a custom message converter; if omitted, a SimpleMessageConverter is used.
	 * If 'jsonConverter', a Jackson2JsonMessageConverter bean will be created for you.
	 */
	private String converterBeanName;

	/**
	 * When true, use a separate connection based on the boot properties.
	 */
	private boolean ownConnection;

	public String getExchange() {
		return this.exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public Expression getExchangeExpression() {
		return this.exchangeExpression;
	}

	public void setExchangeExpression(Expression exchangeExpression) {
		this.exchangeExpression = exchangeExpression;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public Expression getRoutingKeyExpression() {
		return this.routingKeyExpression;
	}

	public void setRoutingKeyExpression(Expression routingKeyExpression) {
		this.routingKeyExpression = routingKeyExpression;
	}

	public boolean getPersistentDeliveryMode() {
		return this.persistentDeliveryMode;
	}

	public void setPersistentDeliveryMode(boolean persistentDeliveryMode) {
		this.persistentDeliveryMode = persistentDeliveryMode;
	}

	public String[] getMappedRequestHeaders() {
		return this.mappedRequestHeaders;
	}

	public void setMappedRequestHeaders(String[] mappedRequestHeaders) {
		this.mappedRequestHeaders = mappedRequestHeaders;
	}

	public String getConverterBeanName() {
		return this.converterBeanName;
	}

	public void setConverterBeanName(String converterBeanName) {
		this.converterBeanName = converterBeanName;
	}

	@AssertTrue(message = "routingKey or routingKeyExpression is required")
	public boolean isRoutingKeyProvided() {
		return this.routingKey != null || this.routingKeyExpression != null;
	}

	public boolean isOwnConnection() {
		return this.ownConnection;
	}

	public void setOwnConnection(boolean ownConnection) {
		this.ownConnection = ownConnection;
	}

}