//
//  AuthorisationException.java
//  storedata
//
//  Created by William Shakour (billy1380) on 4 Dec 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.exception;

import io.reflection.app.api.shared.ApiError;
import io.reflection.app.datatypes.shared.Permission;
import io.reflection.app.datatypes.shared.Role;
import io.reflection.app.datatypes.shared.User;

import java.util.Arrays;
import java.util.List;

import com.willshex.gson.json.service.server.ServiceException;

/**
 * @author billy1380
 * 
 */
@SuppressWarnings("serial")
public class AuthorisationException extends ServiceException {

	private User mUser;
	private List<Permission> mPermissions;
	private List<Role> mRoles;

	/**
	 * 
	 * @param user
	 * @param permission
	 */
	public AuthorisationException(User user, Permission... permission) {
		super(ApiError.MissingPermissions.getCode(), ApiError.MissingPermissions.getMessage());

		mUser = user;

		if (permission != null) {
			// the list created by Arrays.asList is not modifiable
			mPermissions = Arrays.asList(permission);
		}

	}

	/**
	 * 
	 * @param user
	 * @param role
	 */
	public AuthorisationException(User user, Role... role) {
		super(ApiError.MissingRoles.getCode(), ApiError.MissingRoles.getMessage());

		mUser = user;

		if (role != null) {
			mRoles = Arrays.asList(role);
		}
	}

	public AuthorisationException(User user, List<Role> roles, List<Permission> permissions) {
		super(ApiError.MissingRolesAndOrPermissions.getCode(), ApiError.MissingRolesAndOrPermissions.getMessage());

		mUser = user;
		mRoles = roles;
		mPermissions = permissions;
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

		if (mUser.id != null) {
			description.append("id=");
			description.append(mUser.id.toString());
			description.append(",");
		}

		if (mUser.username != null) {
			description.append("username=");
			description.append(mUser.username);
		}

		description.append("]");

		if (mPermissions != null) {
			description.append(" permissions [");
			for (Permission permission : mPermissions) {
				description.append(permission.name);
				description.append(",");
			}
			description.append("]");
		}

		if (mRoles != null) {
			description.append(" roles [");
			for (Role role : mRoles) {
				description.append(role.name);
				description.append(",");
			}
			description.append("]");
		}

		return description.toString();
	}
}
