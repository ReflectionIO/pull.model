//  
//  DataAccountFetchStatusType.java
//  reflection.io
//
//  Created by William Shakour on January 14, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum DataAccountFetchStatusType {
	DataAccountFetchStatusTypeGathered("gathered"),
	DataAccountFetchStatusTypeEmpty("empty"),
	DataAccountFetchStatusTypeError("error"),
	DataAccountFetchStatusTypeIngested("ingested"), ;
	private String value;
	private static Map<String, DataAccountFetchStatusType> valueLookup = null;

	public String toString() {
		return value;
	}

	private DataAccountFetchStatusType(String value) {
		this.value = value;
	}

	public static DataAccountFetchStatusType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, DataAccountFetchStatusType>();
			for (DataAccountFetchStatusType currentDataAccountFetchStatusType : DataAccountFetchStatusType.values()) {
				valueLookup.put(currentDataAccountFetchStatusType.value, currentDataAccountFetchStatusType);
			}
		}
		return valueLookup.get(value);
	}
}