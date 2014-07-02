//
//  DatabaseServiceFactory.java
//  repackagables
//
//  Created by William Shakour on Jun 14, 2012.
//  Copyright © 2012 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.repackaged.scphopr.service.database;

public final class DatabaseServiceFactory {

	public static IDatabaseService createNewDatabaseService() {
		return new DatabaseService();
	}
}
