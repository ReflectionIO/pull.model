//
//  Program.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 10 Jun 2014.
//  Copyright © 2014 Reflection.io. All rights reserved.
//
package io.reflection.pullmodel;

import io.reflection.app.api.admin.shared.call.TriggerPredictRequest;
import io.reflection.app.api.admin.shared.call.TriggerPredictResponse;
import io.reflection.app.api.core.shared.call.LoginRequest;
import io.reflection.app.api.core.shared.call.LoginResponse;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.ApiError;
import io.reflection.app.api.shared.datatypes.Session;
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
import io.reflection.app.shared.util.DataTypeHelper;
import io.reflection.pullmodel.json.service.client.AdminService;
import io.reflection.pullmodel.json.service.client.CoreService;
import io.reflection.pullmodel.json.service.client.JsonService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.willshex.gson.json.service.shared.StatusType;

/**
 * @author billy1380
 * 
 */
public class Program {
	private static final Logger LOGGER = LoggerFactory.getLogger(Program.class);

	private static final String APPLICATION_NAME = "storedatacollector";

	private static final String PROJECT_NAME = "storedatacollector";

	private static String MODEL_QUEUE_NAME = "model";
	// private static String PREDICT_QUEUE_NAME = "predict";

	private static final int DEFAULT_LEASE_DURATION_SECONDS = 60 * 60;
	private static final int DEFAULT_TASKS_TO_LEASE = 1;

	private static final long DEFAULT_ITEM_REFRESH_MILLIS = 12 * 60 * 60 * 1000;
	private static final long DEFAULT_SLEEP_MILLIS = 5 * 60 * 1000;

	private static int leaseCount = DEFAULT_TASKS_TO_LEASE;
	private static int leaseSeconds = DEFAULT_LEASE_DURATION_SECONDS;
	private static Boolean isComputeEngine = null;

	private static int IS_COMPUTE_ENGINE_ARG_INDEX = 0;
	private static int LEASE_SECONDS_ARG_INDEX = 1;
	private static int LEASE_COUNT_ARG_INDEX = 2;

	private static final File DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/pull_model_config");

	private static FileDataStoreFactory dataStoreFactory;
	private static HttpTransport httpTransport;

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static Map<String, String> itemIapLookup = new HashMap<String, String>();
	private static Date itemIapLookupLastUpdated;

	// private static final String TRUNCATED_OUTPUT_PATH =
	// "outputtruncated.csv";
	private static final String ROBUST_OUTPUT_PATH = "outputrobust.csv";

	private static final String FILE_SPARATOR = System.getProperty("file.separator");

	// private static final String CUT_POINT_OUTPUT = "cut.point";
	// private static final String NUMBER_OF_APPS_OUTPUT = "Napps";

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

	private static Session session = null;

	public static void main(String[] args) throws Exception {

		SystemConfigurator.get().configure();

		parseParams(args);

		runModelTasks();
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

		LOGGER.debug("Authenticating");
		Credential credential = authorize();

		LOGGER.debug("Initialising task queue api");
		Taskqueue taskQueueApi = new Taskqueue.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
		        .setTaskqueueRequestInitializer(new TaskqueueRequestInitializer() {
			        public void initializeTaskqueueRequest(TaskqueueRequest<?> request) {
				        request.setPrettyPrint(Boolean.valueOf(true));
			        }
		        }).build();

		// dummyInsert(taskQueueApi);

		LOGGER.debug("Getting model queue");
		TaskQueue modelQueue = getQueue(taskQueueApi, MODEL_QUEUE_NAME);
		LOGGER.error(modelQueue == null ? "modelqueue coun not be obtained" : String.format("modelqueue obtained at [%s]", modelQueue.toString()));

		// TaskQueue predictQueue = getQueue(taskQueueApi, PREDICT_QUEUE_NAME);
		// LOGGER.info(predictQueue);

		LOGGER.info("pulling message from the model");
		while (true) {
			Tasks tasks = getLeasedTasks(taskQueueApi, MODEL_QUEUE_NAME);

			if (tasks == null || tasks.getItems() == null || tasks.getItems().size() == 0) {
				// LOGGER.info("No tasks to lease exiting");
				// break;

				LOGGER.info("No tasks to lease sleeping for a while");
				Thread.sleep(DEFAULT_SLEEP_MILLIS);
			} else {
				for (Task leasedTask : tasks.getItems()) {
					loadItemsIaps();

					LOGGER.info("run R script");
					Map<String, String> mappedParams = new HashMap<String, String>();

					String parameters = leasedTask.getPayloadBase64();
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

					Country c = new Country();
					c.a2Code = country;

					Store s = new Store();
					s.a3Code = store;

					Category category = CategoryServiceProvider.provide().getAllCategory(s);

					Collector collector = CollectorFactory.getCollectorForStore(store);
					List<String> listTypes = new ArrayList<String>();
					listTypes.addAll(collector.getCounterpartTypes(type));
					listTypes.add(type);

					if (executeModelTask(leasedTask, s, c, type, listTypes, category, code)) {
						try {
							callApiTriggerPredict(s, c, type, listTypes, code);

							LOGGER.info("Deleting successfully complete model task");
							deleteTask(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);
						} catch (Throwable caught) {
							LOGGER.error("Could not complete model task - expiring lease", caught);
							expireTaskLease(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);
						}
					} else {
						LOGGER.error("Could not complete model task");
						expireTaskLease(taskQueueApi, leasedTask, MODEL_QUEUE_NAME);
					}
				}
			}
		}
	}

	private static void callApiTriggerPredict(final Store store, final Country country, final String type, final List<String> listTypes, final Long code)
	        throws InterruptedException, ExecutionException {

		if (session == null) {
			callApiLogin(store, country, type, listTypes, code);
		} else {

			AdminService admin = new AdminService();
			admin.setUrl(System.getProperty(SystemConfigurator.ADMIN_SERVICE_URL_KEY));

			final TriggerPredictRequest input = new TriggerPredictRequest();
			input.accessCode = System.getProperty(SystemConfigurator.CLIENT_API_TOKEN_KEY);
			input.session = getSessionForApiCall();
			input.code = code;
			input.country = country;
			input.store = store;
			input.listTypes = listTypes;

			admin.triggerPredict(input, new JsonService.AsyncCallback<TriggerPredictResponse>() {

				@Override
				public void onSuccess(TriggerPredictResponse output) {
					if (output.status == StatusType.StatusTypeSuccess) {
						LOGGER.info(String.format("Triggered predict for store [%s], country [%s], type [%s], code [%d] ", store.a3Code, country.a2Code, type,
						        code.longValue()));
					} else {
						if (output.error != null
						        && output.error.code != null
						        && (ApiError.SessionNoLookup.isCode(output.error.code) || ApiError.SessionNull.isCode(output.error.code) || ApiError.SessionNotFound
						                .isCode(output.error.code))) {
							LOGGER.info("There is an issue with the session, resetting and trigger again");

							session = null;

							try {
								callApiTriggerPredict(store, country, type, listTypes, code);
							} catch (Throwable caught) {
								LOGGER.error("An error occured while calling Api Trigger predict", caught);
								throw new RuntimeException(caught);
							}
						}
					}
				}

				@Override
				public void onFailure(Throwable caught) {
					LOGGER.error("An error occured triggering predict", caught);
					// FIXME: this exception is not caught because it is not on
					// the same thread
					throw new RuntimeException(caught);
				}
			}).get();
		}

	}

	private static Session getSessionForApiCall() {
		Session apiSession = null;

		if (session != null) {
			apiSession = new Session();
			apiSession.token = session.token;
		}

		return apiSession;
	}

	private static void callApiLogin(final Store store, final Country country, final String type, final List<String> listType, final Long code) {
		CoreService core = new CoreService();
		core.setUrl(System.getProperty(SystemConfigurator.CORE_SERVICE_URL_KEY));

		final LoginRequest input = new LoginRequest();
		input.accessCode = System.getProperty(SystemConfigurator.CLIENT_API_TOKEN_KEY);

		input.username = System.getProperty(SystemConfigurator.ADMIN_SERVICE_USERNAME_KEY);
		input.password = System.getProperty(SystemConfigurator.ADMIN_SERVICE_PASSWORD_KEY);

		input.longTerm = Boolean.TRUE;

		try {
			core.login(input, new JsonService.AsyncCallback<LoginResponse>() {

				@Override
				public void onSuccess(LoginResponse output) {
					if (output.status == StatusType.StatusTypeSuccess) {
						if (output.session != null) {
							session = output.session;

							try {
								callApiTriggerPredict(store, country, type, listType, code);
							} catch (Throwable caught) {
								LOGGER.error("An error occured while calling Api Trigger predict", caught);
								throw new RuntimeException(caught);
							}
						} else {
							throw new RuntimeException("Could not login (session is null)");
						}
					} else {
						throw new RuntimeException(String.format("Could not login (%s - %s)",
						        output.error.code == null ? "no error code" : output.error.code.toString(), output.error.message == null ? "no error message"
						                : output.error.message));
					}
				}

				@Override
				public void onFailure(Throwable caught) {
					LOGGER.error("An error occured while logging in", caught);
					// FIXME: this exception is not caught because it is not on the same thread
					throw new RuntimeException(caught);
				}
			}).get();
		} catch (Throwable caught) {
			LOGGER.error("An error occured while calling Api Login", caught);
			throw new RuntimeException(caught);
		}
	}

	// /**
	// * @param taskQueueApi
	// * @throws IOException
	// *
	// */
	// private static void dummyInsert(Taskqueue taskQueueApi) throws
	// IOException {
	//
	// StringBuffer sb = new StringBuffer();
	// sb.setLength(0);
	//
	// Map<String, String> mappedParams = new HashMap<String, String>();
	// mappedParams.put("country", "us");
	// mappedParams.put("store", "ios");
	// mappedParams.put("type", "topfreeapplications");
	// mappedParams.put("code", "892");
	//
	// Task predictTask = new Task();
	//
	// sb.append("/");
	// sb.append(PREDICT_QUEUE_NAME);
	// sb.append("?country=");
	// sb.append(mappedParams.get("country"));
	// sb.append("&store=");
	// sb.append(mappedParams.get("store"));
	// sb.append("&type=");
	// sb.append(mappedParams.get("type"));
	// sb.append("&code=");
	// sb.append(mappedParams.get("code"));
	//
	// predictTask.setQueueName(PREDICT_QUEUE_NAME);
	// predictTask.setPayloadBase64(new
	// String(Base64.encodeBase64(sb.toString().getBytes())));
	//
	// insertTask(taskQueueApi, predictTask, PREDICT_QUEUE_NAME);
	//
	// throw new RuntimeException("Stopped dummy");
	//
	// }
	//
	// public static void dummyInsert1() throws InterruptedException,
	// ExecutionException {
	// Map<String, String> mappedParams = new HashMap<String, String>();
	// mappedParams.put("country", "us");
	// mappedParams.put("store", "ios");
	// mappedParams.put("type", "topfreeapplications");
	// mappedParams.put("code", "892");
	//
	// callApiTriggerPredict(mappedParams);
	// }

	public static boolean parseParams(String[] args) {
		try {
			isComputeEngine = Boolean.parseBoolean(args[IS_COMPUTE_ENGINE_ARG_INDEX]);
			leaseSeconds = Integer.parseInt(args[LEASE_SECONDS_ARG_INDEX]);
			leaseCount = Integer.parseInt(args[LEASE_COUNT_ARG_INDEX]);

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
		Taskqueue.Tasks.Lease leaseRequest = taskQueue.tasks().lease(PROJECT_NAME, taskQueueName, Integer.valueOf(leaseCount), Integer.valueOf(leaseSeconds));
		return (Tasks) leaseRequest.execute();
	}

	/**
	 * @param task
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws DataAccessException
	 */
	private static boolean executeModelTask(Task task, Store store, Country country, String type, List<String> listTypes, Category category, Long code)
	        throws IOException, URISyntaxException, DataAccessException {

		boolean success = false;

		// Date date =
		// RankServiceProvider.provide().getCodeLastRankDate(code);

		try {

			// String freeFilePath = createInputFile(s, c, listTypes, date,
			// "`price`=0", freeFileRef);
			// String paidFilePath = createInputFile(s, c, listTypes, date,
			// "`price`<>0", paidFileRef);
			String freeFilePath = createInputFile(store, country, category, type, listTypes, code, "`price`=0", "free");
			String paidFilePath = createInputFile(store, country, category, type, listTypes, code, "`price`<>0", "paid");

			Modeller modeller = ModellerFactory.getModellerForStore(store.a3Code);

			String outputPath = contextBasedName(ROBUST_OUTPUT_PATH, store.a3Code, country.a2Code, type, code.toString());

			// runRScriptWithParameters("model.R", freeFilePath,
			// paidFilePath, TRUNCATED_OUTPUT_PATH, "400", "40", "500000");
			runRScriptWithParameters("robustModel.R", freeFilePath, paidFilePath, outputPath, "200", "40", "500000", "1e4", "1e3", "270");

			persistValues(outputPath, store, country, modeller.getForm(type), code);

			alterFeedFetchStatus(store, country, category, listTypes, code);

			// deleteFile(TRUNCATED_OUTPUT_PATH);
			deleteFile(outputPath);

			deleteFile(freeFilePath);
			deleteFile(DoneHelper.getDoneFileName(freeFilePath));

			deleteFile(paidFilePath);
			deleteFile(DoneHelper.getDoneFileName(paidFilePath));

			success = true;
		} catch (Exception e) {
			LOGGER.error("Error running script", e);
			LOGGER.error(String.format("Error occured calculating values with parameters store [%s], country [%s], type [%s], [%s]", store, country, type,
			        code == null ? "null" : code.toString()), e);
		}

		return success;
	}

	/**
	 * @param string
	 * @param parameters
	 * @return
	 */
	private static String contextBasedName(String name, String... parameters) {
		StringBuffer sb = new StringBuffer();

		for (String parameter : parameters) {
			sb.append(parameter);
			sb.append("_");
		}

		sb.append(name);

		return sb.toString();
	}

	// private static String createInputFile(Store store, Country country,
	// List<String> listTypes, Date date, String priceQuery, String fileRef)
	private static String createInputFile(Store store, Country country, Category category, String type, List<String> listTypes, Long code, String priceQuery,
	        String fileRef) throws IOException, DataAccessException {
		String inputFilePath = contextBasedName(fileRef, store.a3Code, country.a2Code, type, code.toString()) + ".csv";

		boolean createFile = false;

		if (new File(inputFilePath).exists()) {
			if (new File(DoneHelper.getDoneFileName(inputFilePath)).exists()) {
				// do nothing
			} else {
				deleteFile(inputFilePath);
				createFile = true;
			}
		} else {
			if (new File(DoneHelper.getDoneFileName(inputFilePath)).exists()) {
				deleteFile(DoneHelper.getDoneFileName(inputFilePath));
			}

			createFile = true;
		}

		if (createFile) {
			FileWriter writer = null;

			String typesQueryPart = null;
			if (listTypes.size() == 1) {
				typesQueryPart = String.format("`type`='%s'", listTypes.get(0));
			} else {
				typesQueryPart = "`type` IN ('" + StringUtils.join(listTypes, "','") + "')";
			}

			// String query = String
			// .format("SELECT `r`.`itemid`, `r`.`position`,`r`.`grossingposition`, `r`.`price` FROM `rank` AS `r` WHERE `r`.`country`='%s' AND `r`.`categoryid`=%d AND `r`.`source`='%s' AND %s AND `r`.%s AND `date`<FROM_UNIXTIME(%d)"
			// + " ORDER BY `date` DESC", country.a2Code,
			// category.id.longValue(),
			// store.a3Code, priceQuery, typesQueryPart, date.getTime() / 1000);

			String query = String
			        .format("SELECT `r`.`itemid`, `r`.`date`, `r`.`position`,`r`.`grossingposition`, `r`.`price` FROM `rank` AS `r` WHERE `r`.`country`='%s' AND `r`.`categoryid`=%d AND `r`.`source`='%s' AND %s AND `r`.%s AND `code2`<=%d",
			                country.a2Code, category.id.longValue(), store.a3Code, priceQuery, typesQueryPart, code.longValue());

			Connection rankConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeRank.toString());

			try {
				rankConnection.connect();
				rankConnection.executeQuery(query.toString());

				writer = new FileWriter(inputFilePath);

				writer.append("#item id,date,top position,grossing position,price,usesiap");

				String itemId;

				while (rankConnection.fetchNextRow()) {
					writer.append("\n");

					writer.append("\"");
					writer.append(itemId = rankConnection.getCurrentRowString("itemid"));
					writer.append("\",");

					writer.append((new SimpleDateFormat("yyyy-MM-dd")).format(rankConnection.getCurrentRowDateTime("date")));
					writer.append(",");

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

				DoneHelper.writeDoneFile(inputFilePath);

			} finally {
				if (rankConnection != null) {
					rankConnection.disconnect();
				}

				if (writer != null) {
					writer.close();
				}
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

	// private static void insertTask(Taskqueue taskQueue, Task task, String
	// taskQueueName) throws IOException {
	// Taskqueue.Tasks.Insert request = taskQueue.tasks().insert(PROJECT_NAME,
	// taskQueueName, task);
	// request.execute();
	// }

	private static void deleteTask(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Delete request = taskQueue.tasks().delete("s~" + PROJECT_NAME, taskQueueName, task.getId());
		request.execute();
	}

	private static void expireTaskLease(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		// this is a workaround for an issue with the task queue api
		task.setQueueName(taskQueueName);
		Taskqueue.Tasks.Update request = taskQueue.tasks().update("s~" + PROJECT_NAME, taskQueueName, task.getId(), Integer.valueOf(1), task);
		request.execute();
	}

	private static boolean expiredLookup() {
		boolean expiredLookup = false;

		if (itemIapLookupLastUpdated == null || (new Date()).getTime() - itemIapLookupLastUpdated.getTime() > DEFAULT_ITEM_REFRESH_MILLIS) {
			expiredLookup = true;
		}

		return expiredLookup;
	}

	private static boolean overwriteItem(String existingIapState, String iapState) {
		return existingIapState == null || (!existingIapState.equals(iapState) && !"NA".equals(iapState));
	}

	private static void loadItemsIaps() throws DataAccessException {
		LOGGER.debug("Load iaps if enough time has passed");
		
		if (itemIapLookup.isEmpty() || expiredLookup()) {
			String getItemPropertiesQuery = "SELECT DISTINCT `internalid`, `properties` FROM `item` WHERE `properties` <> 'null' AND NOT `properties` IS NULL AND `deleted`='n' ORDER BY `id` DESC";

			Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

			try {
				itemConnection.connect();
				itemConnection.executeQuery(getItemPropertiesQuery);

				String jsonProperties, itemId;

				while (itemConnection.fetchNextRow()) {
					itemId = itemConnection.getCurrentRowString("internalid");
					jsonProperties = itemConnection.getCurrentRowString("properties");

					String iapState = DataTypeHelper.jsonPropertiesIapState(jsonProperties, "1", "0", "NA");

					if (overwriteItem(itemIapLookup.get(itemId), iapState)) {
						itemIapLookup.put(itemId, iapState);
					}
				}

				itemIapLookupLastUpdated = new Date();
			} finally {
				if (itemConnection != null) {
					itemConnection.disconnect();
				}
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
	 * @param category
	 * @param listTypes
	 * @param code
	 * @throws DataAccessException
	 */
	private static void alterFeedFetchStatus(Store store, Country country, Category category, List<String> listTypes, Long code) throws DataAccessException {
		List<FeedFetch> feeds = FeedFetchServiceProvider.provide().getGatherCodeFeedFetches(country, store, listTypes, code);

		for (FeedFetch feedFetch : feeds) {
			if (feedFetch.category.id.longValue() == category.id.longValue()) {
				feedFetch.status = FeedFetchStatusType.FeedFetchStatusTypeModelled;
				FeedFetchServiceProvider.provide().updateFeedFetch(feedFetch);
			}
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
	private static void persistValues(String resultsFileName, Store store, Country country, FormType form, Long code) throws DataAccessException, IOException {

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