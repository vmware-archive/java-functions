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

package io.pivotal.java.function.tasklauncher.function;

import java.net.URI;
import java.net.URISyntaxException;

import io.pivotal.java.function.tasklauncher.function.support.OnOAuth2ClientCredentialsEnabled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.common.security.support.OAuth2AccessTokenProvidingClientHttpRequestInterceptor;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.DataFlowTemplate;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientAutoConfiguration;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientProperties;
import org.springframework.cloud.dataflow.rest.util.HttpClientConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.endpoint.DefaultClientCredentialsTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2ClientCredentialsGrantRequest;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.endpoint.OAuth2AccessTokenResponse;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class for the TaskLauncher Data Flow Sink.
 *
 * @author David Turanski
 * @author Gunnar Hillert
 */
@Configuration
@EnableConfigurationProperties({TaskLauncherFunctionProperties.class, CustomDataFlowClientProperties.class , DataFlowClientProperties.class})
public class TaskLauncherFunctionConfiguration {

	private static Log logger = LogFactory.getLog(TaskLauncherFunctionConfiguration.class);

	@Autowired(required = false)
	private RestTemplate restTemplate;

	@Value("${autostart:true}")
	private boolean autoStart;

	@Autowired
	private DataFlowClientProperties properties;

	@Autowired
	private CustomDataFlowClientProperties customProperties;


	@Bean
	public TaskLauncherFunction taskLauncherFunction(
		DataFlowOperations dataFlowOperations, TaskLauncherFunctionProperties sinkProperties) {

		if (dataFlowOperations.taskOperations() == null) {
			throw new IllegalArgumentException("The SCDF server does not support task operations");
		}
		TaskLauncherFunction function = new TaskLauncherFunction(dataFlowOperations.taskOperations());
		function.setPlatformName(sinkProperties.getPlatformName());
		return function;
	}

	/**
	 * Once the task-launcher-dataflow sink has been updated for Boot 2.2.x, this bean can be removed
	 * as Data Flow 2.3.x provides this functionality via the {@link DataFlowClientAutoConfiguration}.
	 *
	 * @param clientRegistrations Must not be null if OAuth is enabled
	 * @param clientCredentialsTokenResponseClient Must not be null if OAuth is enabled
	 *
	 * @throws URISyntaxException
	 */
	@Bean
	public DataFlowOperations dataFlowOperations(
			@Autowired(required = false) ClientRegistrationRepository  clientRegistrations,
			@Autowired(required = false) OAuth2AccessTokenResponseClient<OAuth2ClientCredentialsGrantRequest> clientCredentialsTokenResponseClient) throws URISyntaxException {

		final RestTemplate template = DataFlowTemplate.prepareRestTemplate(restTemplate);

		final HttpClientConfigurer httpClientConfigurer = HttpClientConfigurer.create(new URI(properties.getServerUri()))
				.skipTlsCertificateVerification(properties.isSkipSslValidation());

		String accessTokenValue = null;

		if (this.customProperties.getOauth2ClientCredentialsClientId() != null) {
			final String assertionMessage = "%s clientRegistrations must not be null if OAuth2 Client Credentials are being used.";
			Assert.notNull(clientRegistrations, String.format(assertionMessage, clientRegistrations));
			Assert.notNull(clientCredentialsTokenResponseClient, String.format(assertionMessage, clientCredentialsTokenResponseClient));

			final ClientRegistration clientRegistration = clientRegistrations.findByRegistrationId("default");
			final OAuth2ClientCredentialsGrantRequest grantRequest = new OAuth2ClientCredentialsGrantRequest(clientRegistration);
			final OAuth2AccessTokenResponse res = clientCredentialsTokenResponseClient.getTokenResponse(grantRequest);
			accessTokenValue = res.getAccessToken().getTokenValue();
			logger.debug("Configured OAuth2 Client Credentials for accessing the Data Flow Server");
		}
		else if (StringUtils.hasText(this.customProperties.getDataflowServerAccessToken())) {
			accessTokenValue = this.customProperties.getDataflowServerAccessToken();
			logger.debug("Configured OAuth2 Access Token for accessing the Data Flow Server");
		}
		else if (StringUtils.hasText(properties.getAuthentication().getBasic().getUsername())
				&& StringUtils.hasText(properties.getAuthentication().getBasic().getPassword())) {
			httpClientConfigurer.basicAuthCredentials(properties.getAuthentication().getBasic().getUsername(), properties.getAuthentication().getBasic().getPassword());
			logger.debug("Configured basic security for accessing the Data Flow Server");
		}
		else {
			logger.debug("Not configuring basic security for accessing the Data Flow Server");
		}

		if (accessTokenValue != null) {
			template.getInterceptors().add(new OAuth2AccessTokenProvidingClientHttpRequestInterceptor(accessTokenValue));
		}

		template.setRequestFactory(httpClientConfigurer.buildClientHttpRequestFactory());

		return new DataFlowTemplate(new URI(properties.getServerUri()), template);
	}

	@Configuration
	@Conditional(OnOAuth2ClientCredentialsEnabled.class)
	static class clientCredentialsConfiguration {
		@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
		@Bean
		public InMemoryClientRegistrationRepository clientRegistrationRepository(
				CustomDataFlowClientProperties properties) {
			final ClientRegistration clientRegistration = ClientRegistration
					.withRegistrationId("default")
					.authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
					.tokenUri(properties.getOauth2ClientCredentialsTokenUri())
					.clientId(properties.getOauth2ClientCredentialsClientId())
					.clientSecret(properties.getOauth2ClientCredentialsClientSecret())
					.scope(properties.getOauth2ClientCredentialsScopes())
					.build();
			return new InMemoryClientRegistrationRepository(clientRegistration);
		}

		@Bean
		OAuth2AccessTokenResponseClient<OAuth2ClientCredentialsGrantRequest> clientCredentialsTokenResponseClient() {
			return new DefaultClientCredentialsTokenResponseClient();
		}
	}
}
