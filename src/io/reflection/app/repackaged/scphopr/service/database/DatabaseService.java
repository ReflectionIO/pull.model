//
//  DatabaseService.java
//  repackagables
//
//  Created by William Shakour on Jun 14, 2012.
//  Copyright © 2012 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.repackaged.scphopr.service.database;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.service.ServiceType;

/**
 * @author William Shakour
 * 
 */
public class DatabaseService implements IDatabaseService {

	@Override
	public String getName() {
		return ServiceType.ServiceTypeDatabase.toString();
	}

	@Override
	public Connection getConnection(String server, String database, String username, String password) throws DataAccessException {
		return new Connection(server, database, username, password);
	}

	@Override
	public Connection getNamedConnection(String name) throws DataAccessException {
		Connection connection = null;

		String server = System.getProperty(DATABASE_SERVER_KEY), database = System.getProperty(DATABASE_CATALOGUE_KEY), username = System
				.getProperty(DATABASE_USERNAME_KEY), password = System.getProperty(DATABASE_PASSWORD_KEY);
		connection = getConnection(server, database, username, password);

		return connection;
	}

	@Override
	public Connection getAdminNamedConnection(String name) throws DataAccessException {
		Connection connection = null;

		String server = System.getProperty(DATABASE_SERVER_KEY), database = System.getProperty(DATABASE_CATALOGUE_KEY), username = System
				.getProperty(DATABASE_ADMIN_USERNAME_KEY), password = System.getProperty(DATABASE_ADMIN_PASSWORD_KEY);
		connection = getConnection(server, database, username, password);

		return connection;
	}

	@Override
	public Connection getCronNamedConnection(String name) throws DataAccessException {
		Connection connection = null;

		String server = System.getProperty(DATABASE_SERVER_KEY), database = System.getProperty(DATABASE_CATALOGUE_KEY), username = System
				.getProperty(DATABASE_CRON_USERNAME_KEY), password = System.getProperty(DATABASE_CRON_PASSWORD_KEY);
		connection = getConnection(server, database, username, password);

		return connection;
	}

}
