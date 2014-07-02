//  
//  ModelRunServiceProvider.java
//  reflection.io
//
//  Created by William Shakour on October 23, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.modelrun;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class ModelRunServiceProvider {

	/**
	 * @return
	 */
	public static IModelRunService provide() {
		IModelRunService modelRunService = null;

		if ((modelRunService = (IModelRunService) ServiceDiscovery.getService(ServiceType.ServiceTypeModelRun.toString())) == null) {
			modelRunService = ModelRunServiceFactory.createNewModelRunService();
			ServiceDiscovery.registerService(modelRunService);
		}

		return modelRunService;
	}

}