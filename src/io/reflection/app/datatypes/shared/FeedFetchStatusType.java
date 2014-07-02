//  
//  FeedFetchStatusType.java
//  reflection.io
//
//  Created by William Shakour on November 6, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.HashMap;
import java.util.Map;

public enum FeedFetchStatusType {
	FeedFetchStatusTypeGathered("gathered"),
	FeedFetchStatusTypeIngested("ingested"),
	FeedFetchStatusTypeModelled("modelled"),
	FeedFetchStatusTypePredicted("predicted"), ;
	private String value;
	private static Map<String, FeedFetchStatusType> valueLookup = null;

	public String toString() {
		return value;
	}

	private FeedFetchStatusType(String value) {
		this.value = value;
	}

	public static FeedFetchStatusType fromString(String value) {
		if (valueLookup == null) {
			valueLookup = new HashMap<String, FeedFetchStatusType>();
			for (FeedFetchStatusType currentFeedFetchStatusType : FeedFetchStatusType.values()) {
				valueLookup.put(currentFeedFetchStatusType.value, currentFeedFetchStatusType);
			}
		}
		return valueLookup.get(value);
	}
}