//
//  AuthenticationException.java
//  storedata
//
//  Created by William Shakour (billy1380) on 4 Dec 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.exception;

import io.reflection.app.api.shared.ApiError;

import com.willshex.gson.json.service.server.ServiceException;

/**
 * @author billy1380
 * 
 */
@SuppressWarnings("serial")
public class AuthenticationException extends ServiceException {

	private String mUsername;

	/**
	 * 
	 * @param user
	 */
	public AuthenticationException(String username) {
		super(ApiError.InvalidCredentials.getCode(), ApiError.InvalidCredentials.getMessage());

		mUsername = username;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.willshex.gson.json.service.server.ServiceException#toString()
	 */
	@Override
	public String toString() {
		StringBuffer description = new StringBuffer();

		description.append("code[");
		description.append(code);
		description.append("] = ");
		description.append(getClass().getSimpleName());
		description.append("[");
		description.append(getMessage());
		description.append("]");
		description.append(", user[");

		if (mUsername != null) {
			description.append("username=");
			description.append(mUsername);
		}

		description.append("]");

		return description.toString();
	}
}
