//
//  SqlQueryHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 20 Apr 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.helpers;

import java.util.Date;

/**
 * @author billy1380
 * 
 */
public class SqlQueryHelper {

	/**
	 * 
	 * @param before
	 * @param after
	 * @param dateName
	 * @return
	 */
	public static String beforeAfterQuery(Date before, Date after, String dateName) {
		StringBuffer buffer = new StringBuffer();

		if (before != null) {
			before.setTime(before.getTime() + 86399999); // Add 1 day - 1 ms to before
		}

		if (before != null && after != null) {
			buffer.append("(`" + dateName + "` BETWEEN FROM_UNIXTIME(");
			buffer.append(after.getTime() / 1000);
			buffer.append(") AND FROM_UNIXTIME(");
			buffer.append(before.getTime() / 1000);
			buffer.append("))");
		} else if (after != null && before == null) {
			buffer.append("`" + dateName + "`>=FROM_UNIXTIME(");
			buffer.append(after.getTime() / 1000);
			buffer.append(")");
		} else if (before != null && after == null) {
			buffer.append("`" + dateName + "`<FROM_UNIXTIME(");
			buffer.append(before.getTime() / 1000);
			buffer.append(")");
		}

		return buffer.toString();
	}

	/**
	 * 
	 * @param before
	 * @param after
	 * @return
	 */
	public static String beforeAfterQuery(Date before, Date after) {
		return beforeAfterQuery(before, after, "date");
	}

}
