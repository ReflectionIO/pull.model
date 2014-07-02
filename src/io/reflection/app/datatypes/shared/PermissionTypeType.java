//  
//  PermissionTypeType.java
//  reflection.io
//
//  Created by William Shakour on November 21, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum PermissionTypeType {
	PermissionTypeTypeUser("User"),
	PermissionTypeTypeSpecial("Special"), ;
	private String value;
	private static Map<String, PermissionTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private PermissionTypeType(String value) {
		this.value = value;
	}

	public static PermissionTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, PermissionTypeType>();
			for (PermissionTypeType currentPermissionTypeType : PermissionTypeType.values()) {
				valueLookup.put(currentPermissionTypeType.value, currentPermissionTypeType);
			}
		}
		return valueLookup.get(value);
	}
}