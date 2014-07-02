//
//  ResourceHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 18 Feb 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.helpers;

import io.reflection.app.datatypes.shared.Resource;

/**
 * @author billy1380
 * 
 */
public class ResourceHelper {

	public static String getResourceAsString(Resource resource, String formatType) {
		StringBuffer resourceAsString = new StringBuffer();

		if ("plaintext".equalsIgnoreCase(formatType)) {

		} else if ("email".equalsIgnoreCase(formatType)) {

		}

		return resourceAsString.toString();
	}

}
