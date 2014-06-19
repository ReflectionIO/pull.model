//
//  Program.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 10 Jun 2014.
//  Copyright © 2014 Reflection.io. All rights reserved.
//
package io.reflection.pullmodel;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.collectors.Collector;
import io.reflection.app.collectors.CollectorFactory;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.FeedFetch;
import io.reflection.app.datatypes.shared.FeedFetchStatusType;
import io.reflection.app.datatypes.shared.FormType;
import io.reflection.app.datatypes.shared.ModelRun;
import io.reflection.app.datatypes.shared.Store;
import io.reflection.app.modellers.Modeller;
import io.reflection.app.modellers.ModellerFactory;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.service.category.CategoryServiceProvider;
import io.reflection.app.service.feedfetch.FeedFetchServiceProvider;
import io.reflection.app.service.modelrun.ModelRunServiceProvider;
import io.reflection.app.service.rank.RankServiceProvider;
import io.reflection.app.shared.util.DataTypeHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import com.spacehopperstudios.utility.StringUtils;

/**
 * @author billy1380
 * 
 */
public class Program {
	private static final Logger LOGGER = Logger.getLogger(Program.class);

	private static final String APPLICATION_NAME = "storedatacollector";

	private static final String PROJECT_NAME = "storedatacollector";

	private static String MODEL_QUEUE_NAME = "model";
	// private static String PREDICT_QUEUE_NAME = "predict";

	private static final int DEFAULT_LEASE_DURATION = 43200;
	private static final int TASKS_TO_LEASE = 1;

	private static int leaseSecs = DEFAULT_LEASE_DURATION;
	private static Boolean isComputeEngine = null;

	private static final File DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/pull_model_config");

	private static FileDataStoreFactory dataStoreFactory;
	private static HttpTransport httpTransport;

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static Map<String, String> itemIapLookup = new HashMap<String, String>();

	// private static final String TRUNCATED_OUTPUT_PATH =
	// "outputtruncated.csv";
	private static final String ROBUST_OUTPUT_PATH = "outputrobust.csv";

	private static final String FILE_SPARATOR = System.getProperty("file.separator");

//	private static final String CUT_POINT_OUTPUT = "cut.point";
//	private static final String NUMBER_OF_APPS_OUTPUT = "Napps";
	
	private static final String AG_OUTPUT = "ag";
	private static final String AP_OUTPUT = "ap";
	private static final String B_RATIO_OUTPUT = "b.ratio";
	private static final String DT_OUTPUT = "Dt.in";
	private static final String BP_OUTPUT = "bp";
	private static final String BG_OUTPUT = "bg";
	private static final String IAP_AP_OUTPUT = "iap.ap";
	private static final String IAP_AG_OUTPUT = "iap.ag";
	private static final String AF_OUTPUT = "af";
	private static final String TH_OUTPUT = "th";
	private static final String BF_OUTPUT = "bf";

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
			if (tasks != null && tasks.size() > 0) {
				loadItemsIaps();
			}

			for (Task leasedTask : tasks.getItems()) {
				LOGGER.info("run R script");
				if (executeModelTask(leasedTask)) {
					// TODO: insert a perdict task for the process to continue

					LOGGER.info("Deleting successfully complete model task");
					deleteTask(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);
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

	/**
	 * @param task
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws DataAccessException
	 */
	private static boolean executeModelTask(Task task) throws IOException, URISyntaxException, DataAccessException {
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
			Long code = codeParam == null ? null : Long.valueOf(codeParam);

			LOGGER.info("Running task with parameters");

			LOGGER.debug(String.format("store: [%s]", store));
			LOGGER.debug(String.format("country: [%s]", country));
			LOGGER.debug(String.format("type: [%s]", type));
			LOGGER.debug(String.format("code: [%s]", codeParam));

			Date date = RankServiceProvider.provide().getCodeLastRankDate(code);

			try {
				Country c = new Country();
				c.a2Code = country;

				Store s = new Store();
				s.a3Code = store;

				Collector collector = CollectorFactory.getCollectorForStore(store);
				List<String> listTypes = new ArrayList<String>();
				listTypes.addAll(collector.getCounterpartTypes(type));
				listTypes.add(type);

				String freeFilePath = createInputFile(s, c, listTypes, date, "`price`=0", "free");
				String paidFilePath = createInputFile(s, c, listTypes, date, "`price`<>0", "paid");

				Modeller modeller = ModellerFactory.getModellerForStore(store);

				// runRScriptWithParameters("model.R", freeFilePath,
				// paidFilePath, TRUNCATED_OUTPUT_PATH, "400", "40", "500000");
				runRScriptWithParameters("robustModel.R", freeFilePath, paidFilePath, ROBUST_OUTPUT_PATH, "200", "40", "500000");

				persistValues(ROBUST_OUTPUT_PATH, c, s, modeller.getForm(type), code);

				alterFeedFetchStatus(c, s, listTypes, code);

				// deleteFile(TRUNCATED_OUTPUT_PATH);
				deleteFile(ROBUST_OUTPUT_PATH);

				deleteFile(freeFilePath);
				deleteFile(paidFilePath);
			} catch (Exception e) {
				LOGGER.error("Error running script", e);
				LOGGER.fatal(String.format("Error occured calculating values with parameters store [%s], country [%s], type [%s], [%s]", store, country, type,
						date == null ? "null" : Long.toString(date.getTime())), e);
			}
		}

		return false;
	}

	private static String createInputFile(Store store, Country country, List<String> listTypes, Date date, String priceQuery, String fileRef)
			throws IOException, DataAccessException {
		String inputFilePath = fileRef + ".csv";

		FileWriter writer = null;

		String typesQueryPart = null;
		if (listTypes.size() == 1) {
			typesQueryPart = String.format("`type`='%s'", listTypes.get(0));
		} else {
			typesQueryPart = "`type` IN ('" + StringUtils.join(listTypes, "','") + "')";
		}

		Category category = CategoryServiceProvider.provide().getAllCategory(store);

		String query = String
				.format("SELECT `r`.`itemid`, `r`.`position`,`r`.`grossingposition`, `r`.`price` FROM `rank` AS `r` WHERE `r`.`country`='%s' AND `r`.`categoryid`=%d AND `r`.`source`='%s' AND %s AND `r`.%s AND `date`<FROM_UNIXTIME(%d)"
						+ " ORDER BY `date` DESC", country.a2Code, category.id.longValue(), store.a3Code, priceQuery, typesQueryPart, date.getTime() / 1000);

		Connection rankConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeRank.toString());

		try {
			rankConnection.connect();
			rankConnection.executeQuery(query.toString());

			writer = new FileWriter(inputFilePath);

			writer.append("#item id,top position,grossing position,price,usesiap");

			String itemId;

			while (rankConnection.fetchNextRow()) {
				writer.append("\n");

				writer.append("\"");
				writer.append(itemId = rankConnection.getCurrentRowString("itemid"));
				writer.append("\",");

				Integer topPosition = rankConnection.getCurrentRowInteger("position");
				writer.append(topPosition == null || topPosition.intValue() == 0 ? "NA" : topPosition.toString());
				writer.append(",");

				Integer grossingPosition = rankConnection.getCurrentRowInteger("grossingposition");
				writer.append(grossingPosition == null || grossingPosition.intValue() == 0 ? "NA" : grossingPosition.toString());
				writer.append(",");

				double price = rankConnection.getCurrentRowInteger("price").intValue() / 100.0;
				writer.append(Double.toString(price));
				writer.append(",");

				String usesIap = lookupItemIap(itemId);
				writer.append(usesIap);
			}

		} finally {
			if (rankConnection != null) {
				rankConnection.disconnect();
			}

			if (writer != null) {
				writer.close();
			}
		}

		return inputFilePath;
	}

	private static void runRScriptWithParameters(String name, String... parmeters) throws IOException, InterruptedException {
		LOGGER.debug("Entering runRScript");

		StringBuffer command = new StringBuffer();
		command.append("Rscript .");
		command.append(FILE_SPARATOR);
		command.append("R");
		command.append(FILE_SPARATOR);
		command.append(name);

		for (String parameter : parmeters) {
			command.append(" ");
			command.append(parameter);
		}

		LOGGER.debug(String.format("Attempting to run command [%s]", command.toString()));

		Process p = Runtime.getRuntime().exec(command.toString());

		String errLine = null, outLine = null;

		BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream()));

		while ((errLine = err.readLine()) != null) {
			LOGGER.error(errLine);
		}

		while ((outLine = out.readLine()) != null) {
			LOGGER.info(outLine);
		}

		int exitVal = p.waitFor();

		if (exitVal != 0) {
			LOGGER.error("Exited with error code " + exitVal);
		} else {
			LOGGER.info("Exited with error code " + exitVal);
		}

		LOGGER.debug("Exiting runRScript");
	}

	private static void deleteTask(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Delete request = taskQueue.tasks().delete(PROJECT_NAME, taskQueueName, task.getId());
		request.execute();
	}

	private static void expireTaskLease(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Patch request = taskQueue.tasks().patch(PROJECT_NAME, taskQueueName, task.getPayloadBase64(), Integer.valueOf(0), task);
		request.execute();
	}

	private static void loadItemsIaps() throws DataAccessException {

		String getItemPropertiesQuery = "SELECT DISTINCT `internalid`, `properties` FROM `item` WHERE `properties` <> 'null' AND NOT `properties` IS NULL AND `deleted`='n' ORDER BY `id` DESC";

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getItemPropertiesQuery);

			String jsonProperties, itemId;

			while (itemConnection.fetchNextRow()) {
				itemId = itemConnection.getCurrentRowString("internalid");
				jsonProperties = itemConnection.getCurrentRowString("properties");

				itemIapLookup.put(itemId, DataTypeHelper.jsonPropertiesIapState(jsonProperties, "1", "0", "NA"));
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}
	}

	private static String lookupItemIap(String itemId) {
		String usesIap = itemIapLookup.get(itemId);

		// if we have not looked it up
		if (usesIap == null) {
			LOGGER.trace("miss - " + itemId);

			itemIapLookup.put(itemId, usesIap = "NA");
		} else {
			LOGGER.trace("hit - " + itemId);
		}

		LOGGER.trace(usesIap);

		return usesIap;
	}

	public static void deleteFile(String fileFullPath) {
		(new File(fileFullPath)).delete();
	}

	/**
	 * 
	 * @param country
	 * @param store
	 * @param listTypes
	 * @param code
	 * @throws DataAccessException
	 */
	private static void alterFeedFetchStatus(Country country, Store store, List<String> listTypes, Long code) throws DataAccessException {
		List<FeedFetch> feeds = FeedFetchServiceProvider.provide().getGatherCodeFeedFetches(country, store, listTypes, code);

		for (FeedFetch feedFetch : feeds) {
			feedFetch.status = FeedFetchStatusType.FeedFetchStatusTypeModelled;

			FeedFetchServiceProvider.provide().updateFeedFetch(feedFetch);
		}
	}

	/**
	 * @param code
	 * @param listTypes
	 * @param country
	 * @param store
	 * @throws DataAccessException
	 * @throws IOException
	 * 
	 */
	private static void persistValues(String resultsFileName, Country country, Store store, FormType form, Long code) throws DataAccessException, IOException {

		Map<String, String> results = parseOutputFile(resultsFileName);

		ModelRun run = ModelRunServiceProvider.provide().getGatherCodeModelRun(country, store, form, code);

		boolean isUpdate = false;

		if (run == null) {
			run = new ModelRun();
		} else {
			isUpdate = true;
		}

		if (!isUpdate) {
			run.country = country.a2Code;
			run.store = store.a3Code;
			run.code = code;
			run.form = form;
		}

		run.grossingA = Double.valueOf(results.get(AG_OUTPUT));
		run.paidA = Double.valueOf(results.get(AP_OUTPUT));
		run.bRatio = Double.valueOf(results.get(B_RATIO_OUTPUT));
		run.totalDownloads = Double.valueOf(results.get(DT_OUTPUT));
		run.paidB = Double.valueOf(results.get(BP_OUTPUT));
		run.grossingB = Double.valueOf(results.get(BG_OUTPUT));
		run.paidAIap = Double.valueOf(results.get(IAP_AP_OUTPUT));
		run.grossingAIap = Double.valueOf(results.get(IAP_AG_OUTPUT));
		run.freeA = Double.valueOf(results.get(AF_OUTPUT));
		run.theta = Double.valueOf(results.get(TH_OUTPUT));
		run.freeB = Double.valueOf(results.get(BF_OUTPUT));

		if (isUpdate) {
			ModelRunServiceProvider.provide().updateModelRun(run);
		} else {
			ModelRunServiceProvider.provide().addModelRun(run);
		}
	}

	private static Map<String, String> parseOutputFile(String fileFullPath) throws IOException {
		BufferedReader reader = null;
		String line;
		String[] splitLine;
		Map<String, String> parsedVariables = new HashMap<String, String>();
		String parameterName;

		try {
			reader = new BufferedReader(new FileReader(fileFullPath));
			while ((line = reader.readLine()) != null) {

				splitLine = line.split(",");

				if (splitLine != null && splitLine.length == 2) {
					if ((parameterName = splitLine[0].replace("\"", "")).length() != 0) {
						parsedVariables.put(parameterName, splitLine[1]);
					}
				}
			}

		} finally {
			if (reader != null) {
				reader.close();
			}
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(fileFullPath + " parameters");
			for (String key : parsedVariables.keySet()) {
				LOGGER.debug(key + "=" + parsedVariables.get(key));
			}
		}

		return parsedVariables;
	}

}