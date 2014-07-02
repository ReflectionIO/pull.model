//  
//  FormType.java
//  reflection.io
//
//  Created by William Shakour on October 28, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum FormType {
	FormTypeTablet("tablet"),
	FormTypeOther("other"), ;
	private String value;
	private static Map<String, FormType> valueLookup = null;

	public String toString() {
		return value;
	}

	private FormType(String value) {
		this.value = value;
	}

	public static FormType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, FormType>();
			for (FormType currentFormType : FormType.values()) {
				valueLookup.put(currentFormType.value, currentFormType);
			}
		}
		return valueLookup.get(value);
	}
}