/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.tasklaunchrequest.function;

import io.pivotal.java.function.tasklaunchrequest.function.support.CommandLineArgumentsMessageMapper;
import io.pivotal.java.function.tasklaunchrequest.function.support.TaskLaunchRequestSupplier;
import io.pivotal.java.function.tasklaunchrequest.function.support.TaskNameMessageMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;


/**
 * @author David Turanski
 **/
@Configuration
@EnableConfigurationProperties(TaskLaunchRequestFunctionProperties.class)
public class TaskLaunchRequestFunctionConfiguration {

	@Autowired
	private BeanFactory beanFactory;

	private EvaluationContext evaluationContext;

	public final static String TASK_LAUNCH_REQUEST_FUNCTION_NAME = "taskLaunchRequestFunction";

	/**
	 * A {@link java.util.function.Function} to transform a {@link Message} payload to a {@link TaskLaunchRequest}.
	 *
	 * @param taskLaunchRequestMessageProcessor a {@link TaskLaunchRequestMessageProcessor}.
	 *
	 * @return a {code DataFlowTaskLaunchRequest} Message.
	 */
	@Bean(name = TASK_LAUNCH_REQUEST_FUNCTION_NAME)
	public TaskLaunchRequestFunction taskLaunchRequest(TaskLaunchRequestMessageProcessor taskLaunchRequestMessageProcessor) {
		return message -> taskLaunchRequestMessageProcessor.postProcessMessage(message);
	}

	@Bean
	@ConditionalOnMissingBean(TaskNameMessageMapper.class)
	public TaskNameMessageMapper taskNameMessageMapper(TaskLaunchRequestFunctionProperties taskLaunchRequestProperties) {
		if (StringUtils.hasText(taskLaunchRequestProperties.getTaskNameExpression())) {
			SpelExpressionParser expressionParser = new SpelExpressionParser();
			Expression taskNameExpression = expressionParser.parseExpression(taskLaunchRequestProperties.getTaskNameExpression());
			return new ExpressionEvaluatingTaskNameMessageMapper(taskNameExpression, this.evaluationContext);
		}

		return message -> taskLaunchRequestProperties.getTaskName();
	}

	@Bean
	@ConditionalOnMissingBean(CommandLineArgumentsMessageMapper.class)
	public CommandLineArgumentsMessageMapper commandLineArgumentsMessageMapper(
			TaskLaunchRequestFunctionProperties taskLaunchRequestFunctionProperties){
		return new ExpressionEvaluatingCommandLineArgsMapper(taskLaunchRequestFunctionProperties.getArgExpressions(),
				this.evaluationContext);
	}

	@Bean
	public TaskLaunchRequestSupplier taskLaunchRequestInitializer(
			TaskLaunchRequestFunctionProperties taskLaunchRequestProperties){
		return new DataflowTaskLaunchRequestPropertiesInitializer(taskLaunchRequestProperties);
	}

	@Bean
	public TaskLaunchRequestMessageProcessor taskLaunchRequestMessageProcessor(
			TaskLaunchRequestSupplier taskLaunchRequestInitializer,
			TaskNameMessageMapper taskNameMessageMapper,
			CommandLineArgumentsMessageMapper commandLineArgumentsMessageMapper) {

		return new TaskLaunchRequestMessageProcessor(taskLaunchRequestInitializer,
				taskNameMessageMapper,
				commandLineArgumentsMessageMapper);
	}

	static class DataflowTaskLaunchRequestPropertiesInitializer extends TaskLaunchRequestSupplier {
		DataflowTaskLaunchRequestPropertiesInitializer(
				TaskLaunchRequestFunctionProperties taskLaunchRequestProperties){

			this.commandLineArgumentSupplier(
					() -> new ArrayList<>(taskLaunchRequestProperties.getArgs())
			);

			this.deploymentPropertiesSupplier(
					() -> KeyValueListParser.parseCommaDelimitedKeyValuePairs(
							taskLaunchRequestProperties.getDeploymentProperties())
			);

			this.taskNameSupplier(()->taskLaunchRequestProperties.getTaskName());
		}
	}

	static class ExpressionEvaluatingCommandLineArgsMapper implements CommandLineArgumentsMessageMapper {
		private final Map<String,Expression> argExpressionsMap;
		private final EvaluationContext evaluationContext;

		ExpressionEvaluatingCommandLineArgsMapper(String argExpressions, EvaluationContext evaluationContext) {
			this.evaluationContext = evaluationContext;
			this.argExpressionsMap = new HashMap<>();
			if (StringUtils.hasText(argExpressions)) {
				SpelExpressionParser expressionParser = new SpelExpressionParser();

				KeyValueListParser.parseCommaDelimitedKeyValuePairs(argExpressions).forEach(
						(k,v)-> argExpressionsMap.put(k, expressionParser.parseExpression(v)));
			}

		}

		@Override
		public Collection<String> processMessage(Message<?> message) {
			return evaluateArgExpressions(message);
		}

		private Collection<String> evaluateArgExpressions(Message<?> message) {
			List<String> results = new LinkedList<>();
			this.argExpressionsMap.forEach((k, expression) ->
				results.add(String.format("%s=%s", k, expression.getValue(this.evaluationContext, message))));
			return results;
		}
	}

	@PostConstruct
	public void createEvaluationContext(){
		 this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(this.beanFactory);
	}

}
