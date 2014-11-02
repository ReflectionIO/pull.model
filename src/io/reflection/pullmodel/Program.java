//
//  Program.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 10 Jun 2014.
//  Copyright © 2014 Reflection.io. All rights reserved.
//
package io.reflection.pullmodel;

import static com.spacehopperstudios.utility.StringUtils.stripslashes;
import static io.reflection.app.helpers.SqlQueryHelper.beforeAfterQuery;
import static io.reflection.app.service.sale.ISaleService.FREE_OR_PAID_APP_IPAD_IOS;
import static io.reflection.app.service.sale.ISaleService.FREE_OR_PAID_APP_IPHONE_AND_IPOD_TOUCH_IOS;
import static io.reflection.app.service.sale.ISaleService.FREE_OR_PAID_APP_UNIVERSAL_IOS;
import static io.reflection.app.service.sale.ISaleService.INAPP_PURCHASE_PURCHASE_IOS;
import static io.reflection.app.service.sale.ISaleService.INAPP_PURCHASE_SUBSCRIPTION_IOS;
import static io.reflection.app.service.sale.ISaleService.UPDATE_IPAD_IOS;
import static io.reflection.app.service.sale.ISaleService.UPDATE_IPHONE_AND_IPOD_TOUCH_IOS;
import static io.reflection.app.service.sale.ISaleService.UPDATE_UNIVERSAL_IOS;
import static io.reflection.pullmodel.SystemConfigurator.APPLICATION_NAME_KEY;
import static io.reflection.pullmodel.SystemConfigurator.DATA_STORE_NAME_KEY;
import static io.reflection.pullmodel.SystemConfigurator.MODEL_QUEUE_NAME_KEY;
import static io.reflection.pullmodel.SystemConfigurator.PROJECT_NAME_KEY;
import static io.reflection.pullmodel.SystemConfigurator.SECRET_FILE_NAME_KEY;
import static io.reflection.pullmodel.SystemConfigurator.TASK_RETRY_COUNT_KEY;
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
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.ModelRun;
import io.reflection.app.datatypes.shared.ModelTypeType;
import io.reflection.app.datatypes.shared.Rank;
import io.reflection.app.datatypes.shared.Sale;
import io.reflection.app.datatypes.shared.SimpleModelRun;
import io.reflection.app.datatypes.shared.Store;
import io.reflection.app.modellers.Modeller;
import io.reflection.app.modellers.ModellerFactory;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.service.category.CategoryServiceProvider;
import io.reflection.app.service.feedfetch.FeedFetchServiceProvider;
import io.reflection.app.service.modelrun.ModelRunServiceProvider;
import io.reflection.app.service.simplemodelrun.SimpleModelRunServiceProvider;
import io.reflection.app.shared.util.DataTypeHelper;
import io.reflection.pullmodel.json.service.client.AdminService;
import io.reflection.pullmodel.json.service.client.CoreService;
import io.reflection.pullmodel.json.service.client.JsonService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

	// private static String PREDICT_QUEUE_NAME = "predict";

	private static final String MISSING_ID_SKU_PREFIX = "missing -- ";

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
	private static int LOGGER_CONFIG_ARG_INDEX = 3;

	private static FileDataStoreFactory dataStoreFactory;
	private static HttpTransport httpTransport;

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static Map<String, String> itemIapLookup = new HashMap<String, String>();
	private static Date itemIapLookupLastUpdated;

	// private static final String TRUNCATED_OUTPUT_PATH =
	// "outputtruncated.csv";
	private static final String ROBUST_OUTPUT_PATH = "outputrobust.csv";
	private static final String SIMPLE_OUTPUT_PATH = "outputsimple.csv";

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

	private static final String A_OUTPUT = "a";
	private static final String B_OUTPUT = "b";

	private static Session session = null;

	public static void main(String[] args) throws Exception {

		SystemConfigurator.get().configure(args.length > LOGGER_CONFIG_ARG_INDEX ? args[LOGGER_CONFIG_ARG_INDEX] : null);

		parseParams(args);

		runModelTasks();
	}

	private static Credential authorize() throws Exception {
		Credential c = null;

		if (!isComputeEngine.booleanValue()) {
			GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
					new InputStreamReader(new FileInputStream(System.getProperty(SECRET_FILE_NAME_KEY))));
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
		dataStoreFactory = new FileDataStoreFactory(new File(System.getProperty("user.home"), System.getProperty(DATA_STORE_NAME_KEY)));

		LOGGER.debug("Authenticating");
		Credential credential = authorize();

		LOGGER.debug("Initialising task queue api");
		Taskqueue taskQueueApi = new Taskqueue.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(System.getProperty(APPLICATION_NAME_KEY))
				.setTaskqueueRequestInitializer(new TaskqueueRequestInitializer() {
					public void initializeTaskqueueRequest(TaskqueueRequest<?> request) {
						request.setPrettyPrint(Boolean.valueOf(true));
					}
				}).build();

		// dummyInsert(taskQueueApi);

		LOGGER.debug("Getting model queue");
		TaskQueue modelQueue = getQueue(taskQueueApi, System.getProperty(MODEL_QUEUE_NAME_KEY));
		LOGGER.error(modelQueue == null ? "modelqueue coun not be obtained" : String.format("modelqueue obtained at [%s]", modelQueue.toString()));

		// TaskQueue predictQueue = getQueue(taskQueueApi, PREDICT_QUEUE_NAME);
		// LOGGER.info(predictQueue);

		LOGGER.info("pulling message from the model");
		while (true) {

			Tasks tasks = null;
			try {
				tasks = getLeasedTasks(taskQueueApi, System.getProperty(MODEL_QUEUE_NAME_KEY));
			} catch (Exception ex) {
				LOGGER.error("An error occured getting tasks", ex);
			}

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

					String feedFetchIdParam = mappedParams.get("fetchid");
					Long feedFetchId = feedFetchIdParam == null ? null : Long.valueOf(feedFetchIdParam);

					String modelTypeParam = mappedParams.get("modeltype");
					ModelTypeType modelType = modelTypeParam == null ? null : ModelTypeType.fromString(modelTypeParam);

					String store = null;
					String country = null;
					Long categoryId = null;
					String listType = null;
					Long code = null;

					if (feedFetchId == null) {
						store = mappedParams.get("store");
						country = mappedParams.get("country");

						String categoryIdParam = mappedParams.get("categoryid");
						categoryId = categoryIdParam == null ? null : Long.valueOf(categoryIdParam);

						listType = mappedParams.get("type");

						String codeParam = mappedParams.get("code");
						code = codeParam == null ? null : Long.valueOf(codeParam);
					} else {
						FeedFetch feedFetch = FeedFetchServiceProvider.provide().getFeedFetch(feedFetchId);

						if (feedFetch != null) {
							store = feedFetch.store;
							country = feedFetch.country;

							categoryId = feedFetch.category.id;

							listType = feedFetch.type;
							code = feedFetch.code;
						}
					}

					Country c = new Country();
					c.a2Code = country;

					Store s = new Store();
					s.a3Code = store;

					Category category = null;

					if (categoryId == null) {
						category = CategoryServiceProvider.provide().getAllCategory(s);
					} else {
						category = new Category();
						category.id = categoryId;
					}

					Collector collector = CollectorFactory.getCollectorForStore(store);
					List<String> listTypes = new ArrayList<String>();
					listTypes.addAll(collector.getCounterpartTypes(listType));
					listTypes.add(listType);

					Modeller modeller = ModellerFactory.getModellerForStore(store);
					FormType form = modeller.getForm(listType);

					boolean taskCompleted = false;
					SimpleModelRun simpleModelRun = null;

					if (modelType == ModelTypeType.ModelTypeTypeCorrelation) {
						taskCompleted = executeModelTask(leasedTask, modelType, s, c, category, form, listType, listTypes, code);
					} else if (modelType == ModelTypeType.ModelTypeTypeSimple) {
						simpleModelRun = executeSimpleModelTask(leasedTask, modelType, s, c, category, form, listType, listTypes, code);

						if (simpleModelRun != null) {
							taskCompleted = true;
						}
					}

					if (taskCompleted) {
						try {
							callApiTriggerPredict(modelType, s, c, category, listType, listTypes, code, simpleModelRun);

							LOGGER.info("Deleting successfully complete model task");
							deleteTask(taskQueueApi, leasedTask, System.getProperty(MODEL_QUEUE_NAME_KEY));
						} catch (Throwable caught) {
							LOGGER.error("Completed model task but failed to trigger predict api", caught);
							expireTaskUnlessExhaustedRetries(taskQueueApi, leasedTask, System.getProperty(MODEL_QUEUE_NAME_KEY));
						}
					} else {
						LOGGER.error("Could not complete model task");
						expireTaskUnlessExhaustedRetries(taskQueueApi, leasedTask, System.getProperty(MODEL_QUEUE_NAME_KEY));
					}
				}
			}
		}
	}

	private static void expireTaskUnlessExhaustedRetries(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Integer retryCount = Integer.valueOf(System.getProperty(TASK_RETRY_COUNT_KEY));
		if (task.getRetryCount() > retryCount.intValue()) {
			LOGGER.info("Deleting task because it [" + task.getRetryCount().toString() + "] has exceeded allowed retry count [" + retryCount.toString() + "]");
			deleteTask(taskQueue, task, System.getProperty(MODEL_QUEUE_NAME_KEY));
		} else {
			LOGGER.info("Expiring lease");
			try {
				expireTaskLease(taskQueue, task, System.getProperty(MODEL_QUEUE_NAME_KEY));
			} catch (Exception e) {
				LOGGER.error("Failed to expire lease, but moving on", e);
			}
		}
	}

	private static void callApiTriggerPredict(final ModelTypeType modelType, final Store store, final Country country, final Category category,
			final String listType, final List<String> listTypes, final Long code, final SimpleModelRun simpleModelRun) throws InterruptedException,
			ExecutionException {

		if (session == null) {
			callApiLogin(modelType, store, country, category, listType, listTypes, code, simpleModelRun);
		} else {
			AdminService admin = new AdminService();
			admin.setUrl(System.getProperty(SystemConfigurator.ADMIN_SERVICE_URL_KEY));

			final TriggerPredictRequest input = new TriggerPredictRequest();
			input.accessCode = System.getProperty(SystemConfigurator.CLIENT_API_TOKEN_KEY);
			input.session = getSessionForApiCall();
			input.modelType = modelType;

			if (modelType == ModelTypeType.ModelTypeTypeCorrelation) {
				input.code = code;
				input.country = country;
				input.store = store;
				input.listTypes = listTypes;
				input.category = category;
			} else {
				input.simpleModelRun = simpleModelRun;
			}

			admin.triggerPredict(input, new JsonService.AsyncCallback<TriggerPredictResponse>() {

				@Override
				public void onSuccess(TriggerPredictResponse output) {
					if (output.status == StatusType.StatusTypeSuccess) {
						LOGGER.info(String.format("Triggered predict for store [%s], country [%s], type [%s], code [%d] ", store.a3Code, country.a2Code,
								listType, code.longValue()));
					} else {
						if (output.error != null
								&& output.error.code != null
								&& (ApiError.SessionNoLookup.isCode(output.error.code) || ApiError.SessionNull.isCode(output.error.code) || ApiError.SessionNotFound
										.isCode(output.error.code))) {
							LOGGER.info("There is an issue with the session, resetting and trigger again");

							session = null;

							try {
								callApiTriggerPredict(modelType, store, country, category, listType, listTypes, code, simpleModelRun);
							} catch (Throwable caught) {
								LOGGER.error("An error occured while calling Api Trigger predict", caught);
								throw new RuntimeException(caught);
							}
						} else {
							throw new RuntimeException(
									String.format(
											"An error occured while calling Api Trigger predictfor store [%s], country [%s], type [%s], code [%d] with error (%s - %s)",
											store.a3Code, country.a2Code, listType, code.longValue(), output.error.code == null ? "no error code"
													: output.error.code.toString(), output.error.message == null ? "no error message" : output.error.message));
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
			});
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

	private static void callApiLogin(final ModelTypeType modelType, final Store store, final Country country, final Category category, final String listType,
			final List<String> listTypes, final Long code, final SimpleModelRun simpleModelRun) {
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
								callApiTriggerPredict(modelType, store, country, category, listType, listTypes, code, simpleModelRun);
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
			});
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
		Taskqueue.Taskqueues.Get request = taskQueue.taskqueues().get(System.getProperty(PROJECT_NAME_KEY), taskQueueName);
		request.setGetStats(Boolean.valueOf(true));
		return (TaskQueue) request.execute();
	}

	private static Tasks getLeasedTasks(Taskqueue taskQueue, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Lease leaseRequest = taskQueue.tasks().lease(System.getProperty(PROJECT_NAME_KEY), taskQueueName, Integer.valueOf(leaseCount),
				Integer.valueOf(leaseSeconds));
		return (Tasks) leaseRequest.execute();
	}

	/**
	 * 
	 * @param task
	 * @param modelType
	 * @param store
	 * @param country
	 * @param category
	 * @param form
	 * @param listType
	 * @param listTypes
	 * @param code
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws DataAccessException
	 */
	private static boolean executeModelTask(Task task, ModelTypeType modelType, Store store, Country country, Category category, FormType form,
			String listType, List<String> listTypes, Long code) throws IOException, URISyntaxException, DataAccessException {

		boolean success = false;

		// Date date =
		// RankServiceProvider.provide().getCodeLastRankDate(code);

		try {
			String freeFilePath = createInputFile(store, country, category, listType, listTypes, code, "`price`=0", "free");
			String paidFilePath = createInputFile(store, country, category, listType, listTypes, code, "`price`<>0", "paid");

			String outputPath = contextBasedName(ROBUST_OUTPUT_PATH, store.a3Code, country.a2Code, category.id.toString(), listType, code.toString());

			// runRScriptWithParameters("model.R", freeFilePath,
			// paidFilePath, TRUNCATED_OUTPUT_PATH, "400", "40", "500000");
			runRScriptWithParameters("robustModel.R", freeFilePath, paidFilePath, outputPath, "200", "40", "500000", "1e5", "1e9", "1e2");

			persistCorrelationModelValues(outputPath, store, country, form, code);
			alterFeedFetchStatus(store, country, category, listTypes, code);

			// deleteFile(TRUNCATED_OUTPUT_PATH);
			deleteFile(outputPath);

			deleteFile(freeFilePath);
			deleteFile(DoneHelper.getDoneFileName(freeFilePath));

			deleteFile(paidFilePath);
			deleteFile(DoneHelper.getDoneFileName(paidFilePath));

			success = true;
		} catch (Exception e) {
			// LOGGER.error("Error running script", e);
			LOGGER.error(String.format("Error occured calculating values with parameters store [%s], country [%s], type [%s], [%s]", store, country, listType,
					code == null ? "null" : code.toString()), e);
		}

		return success;
	}

	/**
	 * 
	 * @param task
	 * @param modelType
	 * @param store
	 * @param country
	 * @param category
	 * @param form
	 * @param listType
	 * @param listTypes
	 * @param code
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws DataAccessException
	 */
	private static SimpleModelRun executeSimpleModelTask(Task task, ModelTypeType modelType, Store store, Country country, Category category, FormType form,
			String listType, List<String> listTypes, Long code) throws IOException, URISyntaxException, DataAccessException {

		SimpleModelRun simpleModelRun = null;
		boolean success = false;

		// Date date =
		// RankServiceProvider.provide().getCodeLastRankDate(code);

		try {
			String inputFilePath = createSimpleInputFile(store, country, category, listType, listTypes, code);

			String salesInputFilePath = createDeveloperDataSummary(store, country, category, form, listType, code);

			String outputPath = contextBasedName(SIMPLE_OUTPUT_PATH, store.a3Code, country.a2Code, category.id.toString(), form.toString(), code.toString());

			runRScriptWithParameters("simpleModel.R", inputFilePath, salesInputFilePath, Boolean.toString(isDownloadListType(listType)), outputPath);

			FeedFetch feedFetch = FeedFetchServiceProvider.provide().getListTypeCodeFeedFetch(country, store, category, listType, code);

			simpleModelRun = persistSimpleModelValues(outputPath, feedFetch);

			alterFeedFetchStatus(feedFetch);

			deleteFile(outputPath);

			deleteFile(inputFilePath);
			deleteFile(DoneHelper.getDoneFileName(inputFilePath));

			deleteFile(salesInputFilePath);
			deleteFile(DoneHelper.getDoneFileName(salesInputFilePath));

			success = true;

		} catch (Exception e) {
			// LOGGER.error("Error running script", e);
			LOGGER.error(String.format("Error occured calculating values with parameters store [%s], country [%s], type [%s], [%s]", store, country, listType,
					code == null ? "null" : code.toString()), e);
		}

		return success ? simpleModelRun : null;
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

	private static String createSimpleInputFile(Store store, Country country, Category category, String listType, List<String> listTypes, Long code)
			throws IOException, DataAccessException {

		String inputFilePath = contextBasedName(listType, store.a3Code, country.a2Code, category == null || category.id == null ? Long.toString(24)
				: category.id.toString(), code.toString())
				+ ".csv";
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
			String typesQueryPart = null;
			if (listTypes.size() == 1) {
				typesQueryPart = String.format("`type`='%s'", listTypes.get(0));
			} else {
				typesQueryPart = "`type` IN ('" + StringUtils.join(listTypes, "','") + "')";
			}

			String query = String
					.format("SELECT `itemid`, `date`, %s as `position`, `price` FROM `rank` WHERE `code2`=%d AND `country`='%s' AND `categoryid`=%d AND `source`='%s' AND %s",
							isDownloadListType(listType) ? "`position`" : "`grossingposition`", code.longValue(), country.a2Code, category == null
									|| category.id == null ? 24 : category.id.longValue(), store.a3Code, typesQueryPart);

			Connection connection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeRank.toString());

			Map<String, Rank> itemIdRank = new HashMap<String, Rank>();
			boolean gotAllRows = false;

			try {
				connection.connect();
				connection.executeQuery(query.toString());

				Rank rank;
				String itemId;
				Integer position;

				while (connection.fetchNextRow()) {
					if ((position = connection.getCurrentRowInteger("position")).intValue() == 0) continue;

					itemId = connection.getCurrentRowString("itemid");

					if ((rank = itemIdRank.get(itemId)) == null) {
						rank = new Rank();

						rank.itemId = itemId;
						rank.position = position;
						rank.price = Float.valueOf((float) connection.getCurrentRowInteger("price").intValue() / 100.0f);

						itemIdRank.put(itemId, rank);
					} else {
						if (rank.position.intValue() > position.intValue()) {
							rank.position = position;
						}
					}
				}

				gotAllRows = true;
			} finally {
				if (connection != null) {
					connection.disconnect();
				}
			}

			if (gotAllRows) {
				FileWriter writer = null;

				try {
					writer = new FileWriter(inputFilePath);

					writer.append("#item id,position,price,usesiap");

					for (Rank current : itemIdRank.values()) {
						writer.append("\n");

						writer.append("\"");

						writer.append(current.itemId);
						writer.append("\",");

						writer.append(current.position.toString());
						writer.append(",");

						writer.append(current.price.toString());
						writer.append(",");

						String usesIap = lookupItemIap(current.itemId);
						writer.append(usesIap);
					}

					DoneHelper.writeDoneFile(inputFilePath);
				} finally {
					if (writer != null) {
						writer.close();
					}
				}
			}
		}

		return inputFilePath;
	}

	private static String createDeveloperDataSummary(Store store, Country country, Category category, FormType form, String listType, Long code)
			throws IOException, DataAccessException {
		String inputFilePath = contextBasedName("sale", store.a3Code, country.a2Code, form.toString(),
				category == null || category.id == null ? Long.toString(24) : category.id.toString(), code.toString())
				+ ".csv";

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
			boolean gotAllRows = false;
			// ios has a datasource itunes connect
			String dataSourceA3Code = DataTypeHelper.IOS_STORE_A3.equals(store.a3Code) ? "itc" : "*** error ***";

			if (category.name == null) {
				category = CategoryServiceProvider.provide().getCategory(category.id);
			}

			Date startDate = null, endDate = null;
			FeedFetch feedFetch = FeedFetchServiceProvider.provide().getListTypeCodeFeedFetch(country, store, category, listType, code);

			Calendar cal = Calendar.getInstance();
			if (feedFetch != null && feedFetch.date != null) {
				cal.setTime(feedFetch.date);
			} else {
				// ideally we should throw an exception since the code has to be related to the date
				cal.setTime(new Date());
			}

			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 1);

			endDate = cal.getTime();

			cal.add(Calendar.DAY_OF_YEAR, -1);
			startDate = cal.getTime();

			String query = String
					.format("SELECT `itemid`,`sku`,`typeidentifier`,`units`,`customerprice`,`parentidentifier` FROM `sale` JOIN `dataaccount` on "
							+ "`dataaccount`.`id`=`dataaccountid` JOIN `datasource` ON `sourceid`=`datasource`.`id` WHERE `datasource`.`a3code`='%s' AND %s AND `country`='%s'",
							dataSourceA3Code, beforeAfterQuery(endDate, startDate, "begin"), country.a2Code);

			Connection connection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

			List<Sale> sales = new ArrayList<Sale>();
			Map<String, String> parentIdItemIdLookup = new HashMap<String, String>();

			try {
				connection.connect();
				connection.executeQuery(query);

				Sale sale;
				while (connection.fetchNextRow()) {
					sale = new Sale();

					sale.item = new Item();
					sale.item.internalId = connection.getCurrentRowString("itemid");
					sale.typeIdentifier = connection.getCurrentRowString("typeidentifier");
					sale.sku = connection.getCurrentRowString("sku");

					sale.units = connection.getCurrentRowInteger("units");
					sale.customerPrice = connection.getCurrentRowInteger("customerprice");
					sale.parentIdentifier = stripslashes(connection.getCurrentRowString("parentidentifier"));

					if (FREE_OR_PAID_APP_UNIVERSAL_IOS.equals(sale.typeIdentifier) || UPDATE_UNIVERSAL_IOS.equals(sale.typeIdentifier)
							|| (FormType.FormTypeOther == form && (FREE_OR_PAID_APP_IPHONE_AND_IPOD_TOUCH_IOS.equals(sale.typeIdentifier)))
							|| (FormType.FormTypeOther == form && (UPDATE_IPHONE_AND_IPOD_TOUCH_IOS.equals(sale.typeIdentifier)))
							|| (FormType.FormTypeTablet == form && (FREE_OR_PAID_APP_IPAD_IOS.equals(sale.typeIdentifier)))
							|| (FormType.FormTypeTablet == form && (UPDATE_IPAD_IOS.equals(sale.typeIdentifier)))
							|| INAPP_PURCHASE_PURCHASE_IOS.equals(sale.typeIdentifier) || INAPP_PURCHASE_SUBSCRIPTION_IOS.equals(sale.typeIdentifier)) {
						sales.add(sale);
					}

					// If type identifier != IA1 or IA9, add parent identifiers into
					// the Map
					if (!sale.typeIdentifier.equals(INAPP_PURCHASE_PURCHASE_IOS) && !sale.typeIdentifier.equals(INAPP_PURCHASE_SUBSCRIPTION_IOS)) {
						parentIdItemIdLookup.put(sale.sku, sale.item.internalId);
					}
				}

				gotAllRows = true;
			} finally {
				if (connection != null) {
					connection.disconnect();
				}
			}

			if (gotAllRows) {
				String itemId;

				Map<String, Rank> itemIDsRankLookup = new HashMap<String, Rank>();
				Rank rank;
				Set<String> missingIdentifiers = new HashSet<String>();
				for (Sale sale : sales) {
					// Assign item id of the parent to IAP and Subscriptions
					if (sale.typeIdentifier.equals(INAPP_PURCHASE_PURCHASE_IOS) || sale.typeIdentifier.equals(INAPP_PURCHASE_SUBSCRIPTION_IOS)) {
						itemId = parentIdItemIdLookup.get(sale.parentIdentifier);
					} else {
						itemId = sale.item.internalId;
					}

					if (itemId == null) {
						itemId = MISSING_ID_SKU_PREFIX + sale.parentIdentifier;
						missingIdentifiers.add(sale.parentIdentifier);
					}

					if (itemIDsRankLookup.get(itemId) == null) {
						rank = new Rank();
						rank.downloads = Integer.valueOf(0);
						rank.revenue = Float.valueOf(0.0f);

						itemIDsRankLookup.put(itemId, rank);
					} else {
						rank = itemIDsRankLookup.get(itemId);
					}

					// If units and customer prices are negatives (refunds),
					// subtract
					// the value setting units positive
					rank.revenue += (Math.abs(sale.units.floatValue()) * (float) sale.customerPrice.intValue()) / 100.0f;

					// Take into account price and downloads only from main Apps
					if (sale.typeIdentifier.equals(FREE_OR_PAID_APP_IPHONE_AND_IPOD_TOUCH_IOS) || sale.typeIdentifier.equals(FREE_OR_PAID_APP_UNIVERSAL_IOS)
							|| sale.typeIdentifier.equals(FREE_OR_PAID_APP_IPAD_IOS)) {
						rank.downloads += sale.units.intValue();
					}
				}

				FileWriter writer = null;
				try {
					writer = new FileWriter(inputFilePath);

					writer.append("#item id,revenue,downloads");

					for (String key : itemIDsRankLookup.keySet()) {
						rank = itemIDsRankLookup.get(key);

						writer.append("\n");

						writer.append(key);
						writer.append(",");

						writer.append(Double.toString(rank.revenue));
						writer.append(",");

						writer.append(Double.toString(rank.downloads));
					}

					DoneHelper.writeDoneFile(inputFilePath);
				} finally {
					if (writer != null) {
						writer.close();
					}
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

			throw new RuntimeException("Script " + name + " failed with exit code " + exitVal);
		} else {
			LOGGER.info("Exited with error code " + exitVal);
		}

		LOGGER.debug("Exiting runRScript");
	}

	private static void deleteTask(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		Taskqueue.Tasks.Delete request = taskQueue.tasks().delete("s~" + System.getProperty(PROJECT_NAME_KEY), taskQueueName, task.getId());
		request.execute();
	}

	private static void expireTaskLease(Taskqueue taskQueue, Task task, String taskQueueName) throws IOException {
		// this is a workaround for an issue with the task queue api
		task.setQueueName(taskQueueName);
		task.setRetryCount(Integer.valueOf(task.getRetryCount().intValue() + 1));
		Taskqueue.Tasks.Update request = taskQueue.tasks().update("s~" + System.getProperty(PROJECT_NAME_KEY), taskQueueName, task.getId(), Integer.valueOf(1),
				task);
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

	private static void alterFeedFetchStatus(FeedFetch feedFetch) throws DataAccessException {
		feedFetch.status = FeedFetchStatusType.FeedFetchStatusTypeModelled;
		FeedFetchServiceProvider.provide().updateFeedFetch(feedFetch);
	}

	/**
	 * Persist correlation model values
	 * 
	 * @param resultsFileName
	 * @param store
	 * @param country
	 * @param form
	 * @param code
	 * @throws DataAccessException
	 * @throws IOException
	 */
	private static void persistCorrelationModelValues(String resultsFileName, Store store, Country country, FormType form, Long code)
			throws DataAccessException, IOException {
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

	/**
	 * Persist simple model values
	 * 
	 * @param resultsFileName
	 * @param feedFetch
	 * @throws DataAccessException
	 * @throws IOException
	 */
	private static SimpleModelRun persistSimpleModelValues(String resultsFileName, FeedFetch feedFetch) throws DataAccessException, IOException {
		Map<String, String> results = parseOutputFile(resultsFileName);

		SimpleModelRun run = SimpleModelRunServiceProvider.provide().getFeedFetchSimpleModelRun(feedFetch);

		boolean isUpdate = false;

		if (run == null) {
			run = new SimpleModelRun();
		} else {
			isUpdate = false;
		}

		if (!isUpdate) {
			run.feedFetch = feedFetch;
		}

		run.a = Double.valueOf(results.get(A_OUTPUT));
		run.b = Double.valueOf(results.get(B_OUTPUT));

		if (isUpdate) {
			run = SimpleModelRunServiceProvider.provide().updateSimpleModelRun(run);
		} else {
			run = SimpleModelRunServiceProvider.provide().addSimpleModelRun(run);
		}

		return run;

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
						parsedVariables.put(parameterName, splitLine[1].replace("\"", ""));
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

	protected static boolean isDownloadListType(String listType) {
		return !listType.contains("grossing");
	}

}