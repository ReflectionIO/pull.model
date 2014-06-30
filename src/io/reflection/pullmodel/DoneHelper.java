//
//  DoneHelper.java
//  pull.model
//
//  Created by (William Shakour (billy1380)) on 30 Jun 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.pullmodel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author William Shakour (billy1380)
 * 
 */
public class DoneHelper {
	private static final String DONE_FILE_EXTENTION = ".done";

	public static void writeDoneFile(String filePath) throws IOException {
		String doneFilePath = getDoneFileName(filePath);

		FileOutputStream out = null;

		try {
			out = new FileOutputStream(doneFilePath);
			out.write(SimpleDateFormat.getInstance().format(new Date()).getBytes());
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public static Date doneAt(String filePath) throws IOException, ParseException {
		String doneFilePath = getDoneFileName(filePath);
		Date date = null;
		FileInputStream in = null;

		try {
			in = new FileInputStream(doneFilePath);
			byte[] buffer = new byte[1024];
			int read = 0;
			if ((read = in.read(buffer)) > 0) {
				String content = new String(buffer, 0, read);
				date = SimpleDateFormat.getInstance().parse(content);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}

		return date;
	}

	public static String getDoneFileName(String filePath) {
		return filePath + DONE_FILE_EXTENTION;
	}
}
