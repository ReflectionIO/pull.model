//
//  DatabaseServiceProvider.java
//  repackagables
//
//  Created by William Shakour on Jun 14, 2012.
//  Copyright © 2012 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.repackaged.scphopr.service.database;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

public final class DatabaseServiceProvider {

	public static IDatabaseService provide() {
		IDatabaseService databaseService = null;

		if ((databaseService = (IDatabaseService) ServiceDiscovery.getService(ServiceType.ServiceTypeDatabase.toString())) == null) {
			databaseService = DatabaseServiceFactory.createNewDatabaseService();
			ServiceDiscovery.registerService(databaseService);
		}

		return databaseService;
	}

}
