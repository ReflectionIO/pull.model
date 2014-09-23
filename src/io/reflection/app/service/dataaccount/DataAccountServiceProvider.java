//  
//  DataAccountServiceProvider.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.dataaccount;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class DataAccountServiceProvider {

	/**
	 * @return
	 */
	public static IDataAccountService provide() {
		IDataAccountService dataAccountService = null;

		if ((dataAccountService = (IDataAccountService) ServiceDiscovery.getService(ServiceType.ServiceTypeDataAccount.toString())) == null) {
			dataAccountService = DataAccountServiceFactory.createNewDataAccountService();
			ServiceDiscovery.registerService(dataAccountService);
		}

		return dataAccountService;
	}

}