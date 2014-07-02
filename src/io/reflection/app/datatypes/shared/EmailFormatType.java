//  
//  EmailFormatType.java
//  reflection.io
//
//  Created by William Shakour on February 17, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum EmailFormatType {
	EmailFormatTypePlainText("plaintext"),
	EmailFormatTypeHtml("html"),
	EmailFormatTypeRtf("rtf"), ;
	private String value;
	private static Map<String, EmailFormatType> valueLookup = null;

	public String toString() {
		return value;
	}

	private EmailFormatType(String value) {
		this.value = value;
	}

	public static EmailFormatType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, EmailFormatType>();
			for (EmailFormatType currentEmailFormatType : EmailFormatType.values()) {
				valueLookup.put(currentEmailFormatType.value, currentEmailFormatType);
			}
		}
		return valueLookup.get(value);
	}
}