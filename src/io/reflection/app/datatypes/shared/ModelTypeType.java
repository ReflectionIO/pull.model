//  
//  ModelTypeType.java
//  reflection.io
//
//  Created by William Shakour on September 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum ModelTypeType {
	ModelTypeTypeCorrelation("correlation"),
	ModelTypeTypeSimple("simple"), ;
	private String value;
	private static Map<String, ModelTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private ModelTypeType(String value) {
		this.value = value;
	}

	public static ModelTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, ModelTypeType>();
			for (ModelTypeType currentModelTypeType : ModelTypeType.values()) {
				valueLookup.put(currentModelTypeType.value, currentModelTypeType);
			}
		}
		return valueLookup.get(value);
	}
}