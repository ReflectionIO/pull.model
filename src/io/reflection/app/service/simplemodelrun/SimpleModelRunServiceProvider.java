//  
//  SimpleModelRunServiceProvider.java
//  reflection.io
//
//  Created by William Shakour on September 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.simplemodelrun;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class SimpleModelRunServiceProvider {

	/**
	 * @return
	 */
	public static ISimpleModelRunService provide() {
		ISimpleModelRunService simpleModelRunService = null;

		if ((simpleModelRunService = (ISimpleModelRunService) ServiceDiscovery.getService(ServiceType.ServiceTypeSimpleModelRun.toString())) == null) {
			simpleModelRunService = SimpleModelRunServiceFactory.createNewSimpleModelRunService();
			ServiceDiscovery.registerService(simpleModelRunService);
		}

		return simpleModelRunService;
	}

}