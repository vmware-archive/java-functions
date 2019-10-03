/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.cassandra.consumer.cluster;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Scanner;

import javax.annotation.PostConstruct;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.boot.autoconfigure.cassandra.ClusterBuilderCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.cassandra.CassandraReactiveDataAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScanPackages;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.data.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Session;
import reactor.core.publisher.Flux;

/**
 * @author Artem Bilan
 * @author Thomas Risberg
 * @author Rob Hardt
 */
@Configuration
@EnableConfigurationProperties(CassandraClusterProperties.class)
@Import(CassandraAppClusterConfiguration.CassandraPackageRegistrar.class)
public class CassandraAppClusterConfiguration {

	@Bean
	@ConditionalOnProperty(prefix = "cassandra.cluster", name = "createKeyspace")
	public static BeanPostProcessor createKeySpacePostProcessor(CassandraProperties cassandraProperties) {

		return new BeanPostProcessor() {

			@Override
			public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
				if (bean instanceof Cluster) {
					CreateKeyspaceSpecification createKeyspaceSpecification =
							CreateKeyspaceSpecification
									.createKeyspace(cassandraProperties.getKeyspaceName())
									.withSimpleReplication()
									.ifNotExists();

					String createKeySpaceQuery = new CreateKeyspaceCqlGenerator(createKeyspaceSpecification).toCql();

					try (Session session = ((Cluster) bean).connect()) {
						CqlTemplate template = new CqlTemplate(session);
						template.execute(createKeySpaceQuery);
					}
				}
				return bean;
			}
		};

	}

	@Bean
	public ClusterBuilderCustomizer clusterBuilderCustomizer(CassandraClusterProperties cassandraClusterProperties) {
		PropertyMapper map = PropertyMapper.get();
		return builder -> {
			map.from(cassandraClusterProperties::isMetricsEnabled)
					.whenFalse()
					.toCall(builder::withoutMetrics);
			map.from(cassandraClusterProperties::isSkipSslValidation)
					.whenTrue()
					.toCall(() -> {
						RemoteEndpointAwareJdkSSLOptions.Builder optsBuilder =
								RemoteEndpointAwareJdkSSLOptions.builder();
						try {
							optsBuilder.withSSLContext(TrustAllSSLContextFactory.getSslContext());
						}
						catch (NoSuchAlgorithmException | KeyManagementException e) {
							throw new BeanInitializationException(
									"Unable to configure a Cassandra cluster using SSL.", e);
						}
						builder.withSSL(optsBuilder.build());
					});
		};
	}

	static class CassandraPackageRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

		private Environment environment;

		@Override
		public void setEnvironment(Environment environment) {
			this.environment = environment;
		}

		@Override
		public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
				BeanDefinitionRegistry registry) {

			Binder.get(this.environment)
					.bind("cassandra.cluster.entity-base-packages", String[].class)
					.map(Arrays::asList)
					.ifBound(packagesToScan -> EntityScanPackages.register(registry, packagesToScan));
		}

	}

	/**
	 * Inner class to execute init scripts on the provided {@code keyspace}.
	 * It is here to bypass circular dependency with {@link ReactiveCassandraTemplate} injection
	 * and its {@code @amp;Bean} in the {@link CassandraReactiveDataAutoConfiguration}.
	 */
	@Configuration
	protected static class CassandraKeyspaceInitializerConfiguration {

		@Autowired
		private CassandraClusterProperties cassandraClusterProperties;

		@Autowired
		private ReactiveCassandraTemplate reactiveCassandraTemplate;

		@PostConstruct
		public void init() throws IOException {
			if (this.cassandraClusterProperties.getInitScript() != null) {
				String scripts =
						new Scanner(this.cassandraClusterProperties.getInitScript().getInputStream(), "UTF-8")
								.useDelimiter("\\A")
								.next();

				ReactiveCqlOperations reactiveCqlOperations =
						this.reactiveCassandraTemplate.getReactiveCqlOperations();

				Flux.fromArray(StringUtils.delimitedListToStringArray(scripts, ";", "\r\n\f"))
						.filter(StringUtils::hasText) // an empty String after the last ';'
						.flatMap(script -> reactiveCqlOperations.execute(script + ";"))
						.blockLast();
			}
		}

	}

}
