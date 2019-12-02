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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.dataflow.rest.client.DataFlowOperations;
import org.springframework.cloud.dataflow.rest.client.TaskOperations;
import org.springframework.cloud.dataflow.rest.resource.CurrentTaskExecutionsResource;
import org.springframework.cloud.dataflow.rest.resource.LauncherResource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.hateoas.PagedModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = TaslLauncherFunctionTests.TestConfig.class, properties = {
		"spring.main.allow-bean-definition-overriding=true"
})
public class TaslLauncherFunctionTests {

	@Autowired
	private TaskLauncherFunction taskLauncherFunction;

	@Autowired
	private TaskOperations taskOperations;

	@Test
	public void successfulLaunch() {
		LaunchRequest launchRequest = new LaunchRequest();
		launchRequest.setTaskName("someTask");
		setCurrentExecutionState(0);
		Optional<Long> taskId = taskLauncherFunction.apply(launchRequest);
		assertThat(taskId.isPresent()).isTrue();
		assertThat(taskId.get()).isEqualTo(1L);

		verify(taskOperations).launch("someTask",
				Collections.singletonMap(TaskLauncherFunction.TASK_PLATFORM_NAME, "default"),
				Collections.emptyList(),
				null);
	}

	@Test
	public void taskPlatformAtCapacity() {
		LaunchRequest launchRequest = new LaunchRequest();
		launchRequest.setTaskName("someTask");
		setCurrentExecutionState(3);
		Optional<Long> taskId = taskLauncherFunction.apply(launchRequest);
		assertThat(taskId.isPresent()).isFalse();
	}

	@Test
	public void platformMismatch() {
		LaunchRequest launchRequest = new LaunchRequest();
		launchRequest.setTaskName("someTask");
		launchRequest
				.setDeploymentProperties(Collections.singletonMap(TaskLauncherFunction.TASK_PLATFORM_NAME, "other"));
		setCurrentExecutionState(0);
		assertThrows(IllegalStateException.class, () -> taskLauncherFunction.apply(launchRequest));
	}

	private void setCurrentExecutionState(int runningExecutions) {
		CurrentTaskExecutionsResource currentTaskExecutionsResource = new CurrentTaskExecutionsResource();
		currentTaskExecutionsResource.setMaximumTaskExecutions(Integer.valueOf(3));
		currentTaskExecutionsResource.setRunningExecutionCount(runningExecutions);
		currentTaskExecutionsResource.setName("default");
		when(taskOperations.currentTaskExecutions())
				.thenReturn(Collections.singletonList(currentTaskExecutionsResource));
		when(taskOperations.launch(anyString(), anyMap(), anyList(), isNull())).thenReturn(1L);
	}

	@Configuration
	@Import(TaskLauncherFunctionApplication.class)
	static class TestConfig {

		private DataFlowOperations dataFlowOperations = mock(DataFlowOperations.class);

		private TaskOperations taskOperations = mock(TaskOperations.class);

		@Bean
		public TaskOperations taskOperations() {
			List<LauncherResource> launcherResources = new ArrayList<>();
			LauncherResource launcherResource0 = mock(LauncherResource.class);
			when(launcherResource0.getName()).thenReturn("default");
			LauncherResource launcherResource1 = mock(LauncherResource.class);
			when(launcherResource1.getName()).thenReturn("other");
			launcherResources.add(launcherResource0);
			launcherResources.add(launcherResource1);
			when(taskOperations.listPlatforms()).thenReturn(new PagedModel<>(launcherResources, null));
			return taskOperations;
		}

		@Bean
		DataFlowOperations dataFlowOperations(TaskOperations taskOperations) {
			when(dataFlowOperations.taskOperations()).thenReturn(taskOperations);
			return dataFlowOperations;
		}

	}
}
