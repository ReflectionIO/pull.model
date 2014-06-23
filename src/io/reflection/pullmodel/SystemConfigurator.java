//
//  SystemConfigurator.java
//  pull.model
//
//  Created by William Shakour (billy1380) on 16 Jun 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.pullmodel;

import org.apache.log4j.xml.DOMConfigurator;

import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;

/**
 * @author William Shakour (billy1380)
 * 
 */
public class SystemConfigurator {

	private static SystemConfigurator one;

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
		System.setProperty(IDatabaseService.DATABASE_SERVER_KEY, "173.194.104.108");
		System.setProperty(IDatabaseService.DATABASE_CATALOGURE_KEY, "rio");
		System.setProperty(IDatabaseService.DATABASE_USERNAME_KEY, "rio_app_user");
		System.setProperty(IDatabaseService.DATABASE_PASSWORD_KEY, "sooth28@duns");

		System.setProperty(Connection.CONNECTION_NATIVE_KEY, "true");

		DOMConfigurator.configure(LOGGER_CONFIG_PATH);
	}

}
