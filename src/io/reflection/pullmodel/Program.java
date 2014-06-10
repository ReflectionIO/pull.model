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
import java.util.Collections;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

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
	private static final String LOGGER_CONFIG_PATH = "./Logger.xml";
	private static final String APPLICATION_NAME = "storedatacollector";
	
	private static String projectName = "storedatacollector";
	private static String taskQueueName = "model";
	private static int leaseSecs = 43200;
	private static int numTasks = 1;
	
	private static final File DATA_STORE_DIR = new File(
			System.getProperty("user.home"), ".store/pull_model_config");
	
	private static FileDataStoreFactory dataStoreFactory;
	private static HttpTransport httpTransport;
	private static boolean isComputeEngine = true;
	private static final JsonFactory JSON_FACTORY = JacksonFactory
			.getDefaultInstance();

	public static void main(String[] args) throws Exception {
		DOMConfigurator.configure(LOGGER_CONFIG_PATH);

		LOGGER.info("pulling message from the model");
		run();

		LOGGER.info("run R script");

		LOGGER.info("marking message as processed");

		LOGGER.info("putting message into the predict queue");
	}

	private static Credential authorize() throws Exception {
		Credential c = null;

		if (!isComputeEngine) {
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
					JSON_FACTORY,
					new InputStreamReader(Program.class
							.getResourceAsStream("config/secret.json")));
			if ((clientSecrets.getDetails().getClientId().startsWith("Enter"))
					|| (clientSecrets.getDetails().getClientSecret()
							.startsWith("Enter "))) {
				LOGGER.error("Log file not found!");
				System.exit(1);
			}
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
					httpTransport,
					JSON_FACTORY,
					clientSecrets,
					Collections
							.singleton("https://www.googleapis.com/auth/taskqueue"))
					.setDataStoreFactory(dataStoreFactory).build();

			c = new AuthorizationCodeInstalledApp(flow,
					new LocalServerReceiver()).authorize("user");
		} else {
			c = new ComputeCredential(httpTransport, JSON_FACTORY);
		}
		return c;
	}

	private static void run() throws Exception {
		httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

		Credential credential = authorize();

		Taskqueue taskQueue = new Taskqueue.Builder(httpTransport,
				JSON_FACTORY, credential)
				.setApplicationName(APPLICATION_NAME)
				.setTaskqueueRequestInitializer(
						new TaskqueueRequestInitializer() {
							public void initializeTaskqueueRequest(
									TaskqueueRequest<?> request) {
								request.setPrettyPrint(Boolean.valueOf(true));
							}
						}).build();

		TaskQueue queue = getQueue(taskQueue);
		LOGGER.info(queue);

		Tasks tasks = getLeasedTasks(taskQueue);
		if ((tasks.getItems() == null) || (tasks.getItems().size() == 0)) {
			LOGGER.info("No tasks to lease");
		} else {
			for (Task leasedTask : tasks.getItems()) {
				executeTask(leasedTask);
				deleteTask(taskQueue, leasedTask);
			}
		}
	}

	public static boolean parseParams(String[] args) {
		try {
			projectName = args[0];
			taskQueueName = args[1];
			leaseSecs = Integer.parseInt(args[2]);
			numTasks = Integer.parseInt(args[3]);
			
			return true;
		} catch (ArrayIndexOutOfBoundsException ae) {
			System.out.println("Insufficient Arguments");
			return false;
		} catch (NumberFormatException ae) {
			System.out
					.println("Please specify lease seconds and Number of tasks tolease, in number format");
		}
		return false;
	}

	private static TaskQueue getQueue(Taskqueue taskQueue) throws IOException {
		Taskqueue.Taskqueues.Get request = taskQueue.taskqueues().get(
				projectName, taskQueueName);
		request.setGetStats(Boolean.valueOf(true));
		return (TaskQueue) request.execute();
	}

	private static Tasks getLeasedTasks(Taskqueue taskQueue) throws IOException {
		Taskqueue.Tasks.Lease leaseRequest = taskQueue.tasks().lease(
				projectName, taskQueueName, Integer.valueOf(numTasks),
				Integer.valueOf(leaseSecs));
		return (Tasks) leaseRequest.execute();
	}

	private static void executeTask(Task task) throws IOException {
		LOGGER.info("Payload for the task:");
		LOGGER.info(task.getPayloadBase64());

		LOGGER.info("Running task with parameters");
	}

	private static void deleteTask(Taskqueue taskQueue, Task task)
			throws IOException {
		Taskqueue.Tasks.Delete request = taskQueue.tasks().delete(projectName,
				taskQueueName, task.getId());
		request.execute();
	}
}