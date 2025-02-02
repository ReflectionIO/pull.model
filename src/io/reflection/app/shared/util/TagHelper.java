//
//  TagHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 18 Mar 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.shared.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author billy1380
 * 
 */
public class TagHelper {
	/**
	 * Converts a list of comma delimited tags into a tag list
	 * 
	 * @param tags
	 * @return
	 */
	public static List<String> convertToTagList(String tags) {
		List<String> tagList = null;

		if (tags != null && tags.length() >= 0) {
			String[] splitTags = tags.split(",");

			for (String item : splitTags) {
				String tag = item.trim().toLowerCase();

				if (tag.length() > 0) {
					if (tagList == null) {
						tagList = new ArrayList<String>();
					}

					if (!tagList.contains(tag)) {
						tagList.add(tag);
					}
				}
			}
		}

		return tagList;
	}
}
