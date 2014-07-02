//  
//  ForumTypeType.java
//  reflection.io
//
//  Created by William Shakour on April 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum ForumTypeType {
	ForumTypeTypeAnonymous("anonymous"),
	ForumTypeTypePublic("public"),
	ForumTypeTypePrivate("private"), ;
	private String value;
	private static Map<String, ForumTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private ForumTypeType(String value) {
		this.value = value;
	}

	public static ForumTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, ForumTypeType>();
			for (ForumTypeType currentForumTypeType : ForumTypeType.values()) {
				valueLookup.put(currentForumTypeType.value, currentForumTypeType);
			}
		}
		return valueLookup.get(value);
	}
}