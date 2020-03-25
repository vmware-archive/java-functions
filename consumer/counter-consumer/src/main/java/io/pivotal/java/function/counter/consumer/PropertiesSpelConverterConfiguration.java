/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.java.function.counter.consumer;

import java.util.function.Function;

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Christian Tzolov
 */
@Configuration
public class PropertiesSpelConverterConfiguration {

	@Bean
	@ConfigurationPropertiesBinding
	public Converter<String, Expression> propertiesSpelConverter(StringToSpelConversionFunction stringToSpelConversionFunction) {
		return new Converter<String, Expression>() { // NOTE Using lambda causes Java Generics issues.
			@Override
			public Expression convert(String source) {
				return stringToSpelConversionFunction.apply(source);
			}
		};
	}

	@Bean
	public Function<String, Expression> stringToSpelConversionFunction(@Lazy EvaluationContext evaluationContext) {
		return new StringToSpelConversionFunction(evaluationContext);
	}

	/**
	 * Converter from String to Spring Expression.
	 *
	 * TODO: This could be a top level project.
	 */
	public static class StringToSpelConversionFunction implements Function<String, Expression> {

		private SpelExpressionParser parser = new SpelExpressionParser();

		private final EvaluationContext evaluationContext;

		public StringToSpelConversionFunction(EvaluationContext evaluationContext) {
			this.evaluationContext = evaluationContext;
		}

		@Override
		public Expression apply(String source) {
			try {
				Expression expression = parser.parseExpression(source);
				if (expression instanceof SpelExpression) {
					((SpelExpression) expression).setEvaluationContext(evaluationContext);
				}
				return expression;
			}
			catch (ParseException e) {
				throw new IllegalArgumentException(String.format(
						"Could not convert '%s' into a SpEL expression", source), e);
			}
		}
	}
}
