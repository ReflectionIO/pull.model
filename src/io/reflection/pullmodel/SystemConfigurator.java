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

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * @author William Shakour (billy1380)
 */
public class SystemConfigurator {

	private static SystemConfigurator one;

	public static final String ADMIN_SERVICE_USERNAME_KEY = "service.admin.username";
	public static final String ADMIN_SERVICE_PASSWORD_KEY = "service.admin.password";

	public static final String CORE_SERVICE_URL_KEY = "core.service.url";
	public static final String ADMIN_SERVICE_URL_KEY = "admin.service.url";

	public static final String CLIENT_API_TOKEN_KEY = "client.api.token";

	private static final String LOGGER_CONFIG_PATH = "./Logger.xml";

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

		System.setProperty(IDatabaseService.DATABASE_SERVER_KEY, "173.194.104.108");
		System.setProperty(IDatabaseService.DATABASE_CATALOGURE_KEY, "rio");
		System.setProperty(IDatabaseService.DATABASE_USERNAME_KEY, "rio_app_user");
		System.setProperty(IDatabaseService.DATABASE_PASSWORD_KEY, "sooth28@duns");

		System.setProperty(CORE_SERVICE_URL_KEY, "http://www.reflection.io/api/core");
		System.setProperty(ADMIN_SERVICE_URL_KEY, "http://www.reflection.io/api/admin");

		System.setProperty(CLIENT_API_TOKEN_KEY, "931ecd7a-fe45-11e3-8ba6-7054d251af02");

		System.setProperty(ADMIN_SERVICE_USERNAME_KEY, "hello@reflection.io");
		System.setProperty(ADMIN_SERVICE_PASSWORD_KEY, "clattered83-eurobond");

		System.setProperty(Connection.CONNECTION_NATIVE_KEY, "true");
	}

}
