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

import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.dataflow.rest.client.config.DataFlowClientAutoConfiguration;

/**
 * Configuration properties used in {@link DataFlowClientAutoConfiguration}
 *
 * @author Gunnar Hillert
 */
@ConfigurationProperties
public class CustomDataFlowClientProperties {

	/**
	 * OAuth2 Access Token.
	 */
	private String dataflowServerAccessToken;

	/**
	 * The OAuth2 Client Id (Used for the client credentials grant).
	 *
	 */
	private String oauth2ClientCredentialsClientId;

	/**
	 * The OAuth2 Client Secret (Used for the client credentials grant).
	 */
	private String oauth2ClientCredentialsClientSecret;

	/**
	 * Token URI for the OAuth2 provider (Used for the client credentials grant).
	 */
	private String oauth2ClientCredentialsTokenUri;

	/**
	 * OAuth2 Authorization scopes (Used for the client credentials grant).
	 */
	private Set<String> oauth2ClientCredentialsScopes;

	public String getDataflowServerAccessToken() {
		return dataflowServerAccessToken;
	}

	public void setDataflowServerAccessToken(String dataflowServerAccessToken) {
		this.dataflowServerAccessToken = dataflowServerAccessToken;
	}

	public String getOauth2ClientCredentialsClientId() {
		return oauth2ClientCredentialsClientId;
	}

	public void setOauth2ClientCredentialsClientId(String oauth2ClientCredentialsClientId) {
		this.oauth2ClientCredentialsClientId = oauth2ClientCredentialsClientId;
	}

	public String getOauth2ClientCredentialsClientSecret() {
		return oauth2ClientCredentialsClientSecret;
	}

	public void setOauth2ClientCredentialsClientSecret(String oauth2ClientCredentialsClientSecret) {
		this.oauth2ClientCredentialsClientSecret = oauth2ClientCredentialsClientSecret;
	}

	public String getOauth2ClientCredentialsTokenUri() {
		return oauth2ClientCredentialsTokenUri;
	}

	public void setOauth2ClientCredentialsTokenUri(String oauth2ClientCredentialsTokenUri) {
		this.oauth2ClientCredentialsTokenUri = oauth2ClientCredentialsTokenUri;
	}

	public Set<String> getOauth2ClientCredentialsScopes() {
		return oauth2ClientCredentialsScopes;
	}

	public void setOauth2ClientCredentialsScopes(Set<String> oauth2ClientCredentialsScopes) {
		this.oauth2ClientCredentialsScopes = oauth2ClientCredentialsScopes;
	}

}
