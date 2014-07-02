//  
//  ResourceTypeType.java
//  reflection.io
//
//  Created by William Shakour on February 17, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum ResourceTypeType {
	ResourceTypeTypeImage("image"),
	ResourceTypeTypeGoogleCloudServiceImage("googlecloudserviceimage"),
	ResourceTypeTypeYoutubeVideo("youtubevideo"),
	ResourceTypeTypeHtml5Video("html5video"), ;
	private String value;
	private static Map<String, ResourceTypeType> valueLookup = null;

	public String toString() {
		return value;
	}

	private ResourceTypeType(String value) {
		this.value = value;
	}

	public static ResourceTypeType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, ResourceTypeType>();
			for (ResourceTypeType currentResourceTypeType : ResourceTypeType.values()) {
				valueLookup.put(currentResourceTypeType.value, currentResourceTypeType);
			}
		}
		return valueLookup.get(value);
	}
}