//
//  ApiHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 30 May 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.helpers;

import io.reflection.app.collectors.CollectorIOS;
import io.reflection.app.datatypes.shared.Store;

/**
 * @author billy1380
 * 
 */
public class ApiHelper {
	public static String getGrossingListName(Store store, String type) {
		String listName = null;

		if ("ios".equalsIgnoreCase(store.a3Code)) {
			if (type != null && type.contains("ipad") || type.toLowerCase().contains("ipad")) {
				listName = CollectorIOS.TOP_GROSSING_IPAD_APPS;
			} else {
				listName = CollectorIOS.TOP_GROSSING_APPS;
			}
		}

		return listName;
	}
}
