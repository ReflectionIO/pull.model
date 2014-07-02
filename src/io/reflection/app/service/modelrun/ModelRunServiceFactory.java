//  
//  ModelRunServiceFactory.java
//  reflection.io
//
//  Created by William Shakour on October 23, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.modelrun;

final class ModelRunServiceFactory {

	/**
	 * @return
	 */
	public static IModelRunService createNewModelRunService() {
		return new ModelRunService();
	}

}