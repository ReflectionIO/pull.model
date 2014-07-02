//
//  IDatabaseService.java
//  repackagables
//
//  Created by William Shakour on Jun 14, 2012.
//  Copyright © 2012 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.repackaged.scphopr.service.database;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;

import com.spacehopperstudios.service.IService;

public interface IDatabaseService extends IService {

	public static final String DATABASE_SERVER_KEY = "db.connection.server";
	public static final String DATABASE_CATALOGURE_KEY = "db.connection.catalogue";
	public static final String DATABASE_USERNAME_KEY = "db.connection.username";
	public static final String DATABASE_PASSWORD_KEY = "db.connection.password";

	public static final String DATABASE_ADMIN_USERNAME_KEY = "db.connection.admin.username";
	public static final String DATABASE_ADMIN_PASSWORD_KEY = "db.connection.admin.password";

	public static final String DATABASE_CRON_USERNAME_KEY = "db.connection.cron.username";
	public static final String DATABASE_CRON_PASSWORD_KEY = "db.connection.cron.password";

	public Connection getNamedConnection(String name) throws DataAccessException;

	public Connection getConnection(String server, String database, String username, String password) throws DataAccessException;

	public Connection getAdminNamedConnection(String name) throws DataAccessException;

	public Connection getCronNamedConnection(String name) throws DataAccessException;

}
