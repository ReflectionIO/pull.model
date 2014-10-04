//
//  SystemConfigurator.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 16 Jun 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.pullmodel;

import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * @author William Shakour (billy1380)
 */
public class SystemConfigurator {

	private static SystemConfigurator one;

	private static final String LOGGER_CONFIG_PATH = "./Logger.xml";
	private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigurator.class);

	private static final String CONFIG_FILE_PATH_KEY = "io.reflection.pullmodel.config.file";

	public static final String DATABASE_SERVER_DEFAULT = "173.194.104.108";
	public static final String DATABASE_CATALOGUE_DEFAULT = "rio";
	public static final String DATABASE_USERNAME_DEFAULT = "rio_app_user";
	public static final String DATABASE_PASSWORD_DEFAULT = "sooth28@duns";

	public static final String CONNECTION_NATIVE_DEFAULT = "true";

	public static final String ADMIN_SERVICE_USERNAME_KEY = "service.admin.username";
	private static final String ADMIN_SERVICE_USERNAME_DEFAULT = "hello@reflection.io";

	public static final String ADMIN_SERVICE_PASSWORD_KEY = "service.admin.password";
	private static final String ADMIN_SERVICE_PASSWORD_DEFAULT = "clattered83-eurobond";

	public static final String CORE_SERVICE_URL_KEY = "core.service.url";
	private static final String CORE_SERVICE_URL_DEFAULT = "http://www.reflection.io/api/core";

	public static final String ADMIN_SERVICE_URL_KEY = "admin.service.url";
	private static final String ADMIN_SERVICE_URL_DEFAULT = "http://www.reflection.io/api/admin";

	public static final String CLIENT_API_TOKEN_KEY = "client.api.token";
	private static final String CLIENT_API_TOKEN_DEFAULT = "931ecd7a-fe45-11e3-8ba6-7054d251af02";

	public static final String APPLICATION_NAME_KEY = "application.name";
	private static final String APPLICATION_NAME_DEFAULT = "storedatacollector";

	public static final String PROJECT_NAME_KEY = "project.name";
	private static final String PROJECT_NAME_DEFAULT = "storedatacollector";

	public static final String MODEL_QUEUE_NAME_KEY = "model.queue.name";
	private static final String MODEL_QUEUE_NAME_DEFAULT = "model";

	public static final String DATA_STORE_NAME_KEY = "datastore.name";
	private static final String DATA_STORE_NAME_DEFAULT = ".store/pull_model_config";

	public static final String SECRET_FILE_NAME_KEY = "secret.file.name";
	private static final String SECRET_FILE_NAME_DEFAULT = "config/secret.json";

	public static final String TASK_RETRY_COUNT_KEY = "task.retry.count";
	public static final String TASK_RETRY_COUNT_DEFAULT = "1";

	/**
	 * @return
	 */
	public static SystemConfigurator get() {
		if (one == null) {
			one = new SystemConfigurator();
		}

		return one;
	}

	public void configure() {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		DOMConfigurator.configure(LOGGER_CONFIG_PATH);

		String configFile = System.getProperty(CONFIG_FILE_PATH_KEY);

		boolean defaultConfig = (configFile == null || configFile.isEmpty());

		if (!defaultConfig) {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream(configFile));

				for (Entry<Object, Object> entry : properties.entrySet()) {
					System.setProperty(entry.getKey().toString(), entry.getValue().toString());
				}

			} catch (IOException ex) {

				LOGGER.error("Could not open file [" + configFile + "]", ex);

				defaultConfig = true;
			}
		}

		String propertyValue;

		if (defaultConfig || (propertyValue = System.getProperty(IDatabaseService.DATABASE_SERVER_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(IDatabaseService.DATABASE_SERVER_KEY, DATABASE_SERVER_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(IDatabaseService.DATABASE_CATALOGUE_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(IDatabaseService.DATABASE_CATALOGUE_KEY, DATABASE_CATALOGUE_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(IDatabaseService.DATABASE_USERNAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(IDatabaseService.DATABASE_USERNAME_KEY, DATABASE_USERNAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(IDatabaseService.DATABASE_PASSWORD_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(IDatabaseService.DATABASE_PASSWORD_KEY, DATABASE_PASSWORD_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(CORE_SERVICE_URL_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(Connection.CONNECTION_NATIVE_KEY, CONNECTION_NATIVE_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(CORE_SERVICE_URL_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(CORE_SERVICE_URL_KEY, CORE_SERVICE_URL_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(ADMIN_SERVICE_URL_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(ADMIN_SERVICE_URL_KEY, ADMIN_SERVICE_URL_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(CLIENT_API_TOKEN_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(CLIENT_API_TOKEN_KEY, CLIENT_API_TOKEN_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(ADMIN_SERVICE_USERNAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(ADMIN_SERVICE_USERNAME_KEY, ADMIN_SERVICE_USERNAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(ADMIN_SERVICE_PASSWORD_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(ADMIN_SERVICE_PASSWORD_KEY, ADMIN_SERVICE_PASSWORD_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(APPLICATION_NAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(APPLICATION_NAME_KEY, APPLICATION_NAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(PROJECT_NAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(PROJECT_NAME_KEY, PROJECT_NAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(MODEL_QUEUE_NAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(MODEL_QUEUE_NAME_KEY, MODEL_QUEUE_NAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(DATA_STORE_NAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(DATA_STORE_NAME_KEY, DATA_STORE_NAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(SECRET_FILE_NAME_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(SECRET_FILE_NAME_KEY, SECRET_FILE_NAME_DEFAULT);
		}

		if (defaultConfig || (propertyValue = System.getProperty(TASK_RETRY_COUNT_KEY)) == null || propertyValue.isEmpty()) {
			System.setProperty(TASK_RETRY_COUNT_KEY, TASK_RETRY_COUNT_DEFAULT);
		}

	}
}
