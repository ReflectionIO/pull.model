//  
//  EmailTypeType.java
//  reflection.io
//
//  Created by William Shakour on February 18, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum EmailTypeType {
	EmailTypeTypeSystem("system"),
	EmailTypeTypePromotional("promotional"), ;
	private String value;
	private static Map<String, EmailTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private EmailTypeType(String value) {
		this.value = value;
	}

	public static EmailTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, EmailTypeType>();
			for (EmailTypeType currentEmailTypeType : EmailTypeType.values()) {
				valueLookup.put(currentEmailTypeType.value, currentEmailTypeType);
			}
		}
		return valueLookup.get(value);
	}
}