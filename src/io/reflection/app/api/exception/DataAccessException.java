//
// DatabaseException.java
// storedata
//
// Created by William Shakour (billy1380) on 4 Dec 2013.
// Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.exception;

import com.willshex.gson.json.service.server.ServiceException;

/**
 * @author billy1380
 */
@SuppressWarnings("serial")
public class DataAccessException extends ServiceException {

	private Throwable cause;

	public DataAccessException() {
		super(400000, "A database exception occured.");
	}

	public DataAccessException(Throwable cause) {
		super(400000, "A database exception occured.");

		this.cause = cause;
	}

	public Throwable getCause() {
		return cause;
	}

}