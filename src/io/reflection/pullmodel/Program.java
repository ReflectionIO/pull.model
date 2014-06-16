//
//  Program.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 10 Jun 2014.
//  Copyright © 2014 Reflection.io. All rights reserved.
//
package io.reflection.pullmodel;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.taskqueue.Taskqueue;
import com.google.api.services.taskqueue.TaskqueueRequest;
import com.google.api.services.taskqueue.TaskqueueRequestInitializer;
import com.google.api.services.taskqueue.model.Task;
import com.google.api.services.taskqueue.model.TaskQueue;
import com.google.api.services.taskqueue.model.Tasks;

/**
 * @author billy1380
 * 
 */
public class Program {
	private static final Logger LOGGER = Logger.getLogger(Program.class);

	private static final String APPLICATION_NAME = "storedatacollector";

	private static final String PROJECT_NAME = "storedatacollector";

	private static String MODEL_QUEUE_NAME = "model";
	private static String PREDICT_QUEUE_NAME = "predict";

	private static final int DEFAULT_LEASE_DURATION = 43200;
	private static final int TASKS_TO_LEASE = 1;

	private static int leaseSecs = DEFAULT_LEASE_DURATION;
	private static Boolean isComputeEngine = null;

	private static final File DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/pull_model_config");

	private static FileDataStoreFactory dataStoreFactory;
	private static HttpTransport httpTransport;

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	public static void main(String[] args) throws Exception {

		SystemConfigurator.get().configure();

		parseParams(args);

		LOGGER.info("pulling message from the model");
		runModelTasks();

		LOGGER.info("marking message as processed");

		LOGGER.info("putting message into the predict queue");
	}

	private static Credential authorize() throws Exception {
		Credential c = null;

		if (!isComputeEngine.booleanValue()) {
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
					new InputStreamReader(Program.class.getResourceAsStream("config/secret.json")));
			if ((clientSecrets.getDetails().getClientId().startsWith("Enter")) || (clientSecrets.getDetails().getClientSecret().startsWith("Enter "))) {
				LOGGER.error("Log file not found!");
				System.exit(1);
			}
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets,
					Collections.singleton("https://www.googleapis.com/auth/taskqueue")).setDataStoreFactory(dataStoreFactory).build();

			c = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		} else {
			c = new ComputeCredential(httpTransport, JSON_FACTORY);
		}
		return c;
	}

	private static void runModelTasks() throws Exception {
		httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

		Credential credential = authorize();

		Taskqueue taskQueueApi = new Taskqueue.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
				.setTaskqueueRequestInitializer(new TaskqueueRequestInitializer() {
					public void initializeTaskqueueRequest(TaskqueueRequest<?> request) {
						request.setPrettyPrint(Boolean.valueOf(true));
					}
				}).build();

		TaskQueue modelQueue = getQueue(taskQueueApi, MODEL_QUEUE_NAME);
		LOGGER.info(modelQueue);

		// TaskQueue predictQueue = getQueue(taskQueueApi, PREDICT_QUEUE_NAME);
		// LOGGER.info(predictQueue);

		Tasks tasks = getLeasedTasks(taskQueueApi, MODEL_QUEUE_NAME);
		if ((tasks.getItems() == null) || (tasks.getItems().size() == 0)) {
			LOGGER.info("No tasks to lease");
		} else {
			for (Task leasedTask : tasks.getItems()) {
				LOGGER.info("run R script");
				if (executeModelTask(leasedTask)) {
					LOGGER.info("Deleting successfully complete model task");
					deleteTask(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);

					// TODO: insert a perdict task for the process to continue
				} else {
					LOGGER.error("Could not complete model task");
					expireTaskLease(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);
				}
			}
		}
	}

	public static boolean parseParams(String[] args) {
		try {
			isComputeEngine = Boolean.parseBoolean(args[0]);
			leaseSecs = Integer.parseInt(args[1]);

			return true;
		} catch (ArrayIndexOutOfBoundsException ae) {
			LOGGER.error("Insufficient Arguments");
			return false;
		} catch (NumberFormatException ae) {
			LOGGER.error("Please specify lease seconds and Number of tasks tolease, in number format");
		}

		return false;
	}

	private static TaskQueue getQueue(Taskqueue taskQueue, String taskQueueName) throws IOException {
		Taskqueue.Taskqueues.Get request = taskQueue.taskqueues().get(PROJECT_NAME, taskQueueName);
		request.setGetStats(Boolean.valueOf(true));
		return (TaskQueue) request.execute();
	}

	private static Tasks getLeasedTasks(Taskqueue taskQueue, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Lease leaseRequest = taskQueue.tasks().lease(PROJECT_NAME, taskQueueName, Integer.valueOf(TASKS_TO_LEASE), Integer.valueOf(leaseSecs));
		return (Tasks) leaseRequest.execute();
	}

	private static boolean executeModelTask(Task task) throws IOException, URISyntaxException {
		LOGGER.info("Payload for the task:");

		String parameters = task.getPayloadBase64();
		LOGGER.info(parameters);

		Map<String, String> mappedParams = new HashMap<String, String>();

		if (parameters != null) {
			String decodedParameters = new String(Base64.decodeBase64(parameters.getBytes()));

			if (decodedParameters != null) {
				String[] parts = decodedParameters.split("&");

				String[] subParts = null;
				for (String part : parts) {
					subParts = part.split("=");
					mappedParams.put(subParts[0], subParts[1]);
				}
			}

			String store = mappedParams.get("store");
			String country = mappedParams.get("country");
			String type = mappedParams.get("type");
			String codeParam = mappedParams.get("code");
			// Long code = codeParam == null ? null : Long.valueOf(codeParam);

			LOGGER.debug(String.format("store: [%s]", store));
			LOGGER.debug(String.format("country: [%s]", country));
			LOGGER.debug(String.format("type: [%s]", type));
			LOGGER.debug(String.format("code: [%s]", codeParam));

			LOGGER.info("Running task with parameters");
		}

		return false;
	}

	private static void deleteTask(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Delete request = taskQueue.tasks().delete(PROJECT_NAME, taskQueueName, task.getId());
		request.execute();
	}

	private static void expireTaskLease(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Patch request = taskQueue.tasks().patch(PROJECT_NAME, taskQueueName, task.getPayloadBase64(), Integer.valueOf(0), task);
		request.execute();
	}
}