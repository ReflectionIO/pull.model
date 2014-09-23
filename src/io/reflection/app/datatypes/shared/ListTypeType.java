//  
//  ListTypeType.java
//  reflection.io
//
//  Created by William Shakour on September 23, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum ListTypeType {
	ListTypeTypeFree("free"),
	ListTypeTypePaid("paid"),
	ListTypeTypeGrossing("grossing"), ;
	private String value;
	private static Map<String, ListTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private ListTypeType(String value) {
		this.value = value;
	}

	public static ListTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, ListTypeType>();
			for (ListTypeType currentListTypeType : ListTypeType.values()) {
				valueLookup.put(currentListTypeType.value, currentListTypeType);
			}
		}
		return valueLookup.get(value);
	}
}