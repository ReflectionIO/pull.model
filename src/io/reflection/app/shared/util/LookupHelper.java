//
//  LookupHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 19 May 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.shared.util;

import io.reflection.app.datatypes.shared.DataType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author billy1380
 * 
 */
public class LookupHelper {

	public static <T extends DataType> Map<Long, T> makeLookup(Collection<T> collection) {
		Map<Long, T> lookup = new HashMap<Long, T>();

		for (T t : collection) {
			lookup.put(t.id, t);
		}

		return lookup;
	}
}
