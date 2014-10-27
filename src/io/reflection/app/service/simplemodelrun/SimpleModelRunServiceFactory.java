//  
//  SimpleModelRunServiceFactory.java
//  reflection.io
//
//  Created by William Shakour on September 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.simplemodelrun;

final class SimpleModelRunServiceFactory {

	/**
	 * @return
	 */
	public static ISimpleModelRunService createNewSimpleModelRunService() {
		return new SimpleModelRunService();
	}

}