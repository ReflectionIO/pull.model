//  
//  DataAccountServiceFactory.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.dataaccount;

final class DataAccountServiceFactory {

	/**
	 * @return
	 */
	public static IDataAccountService createNewDataAccountService() {
		return new DataAccountService();
	}

}