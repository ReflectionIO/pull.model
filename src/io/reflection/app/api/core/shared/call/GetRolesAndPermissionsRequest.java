//  
//  GetRolesAndPermissionsRequest.java
//  reflection.io
//
//  Created by William Shakour on December 9, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Request;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class GetRolesAndPermissionsRequest extends Request {
	public Boolean idsOnly;
	public Boolean expandRoles;
	public Boolean rolesOnly;
	public Boolean permissionsOnly;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonIdsOnly = idsOnly == null ? JsonNull.INSTANCE : new JsonPrimitive(idsOnly);
		object.add("idsOnly", jsonIdsOnly);
		JsonElement jsonExpandRoles = expandRoles == null ? JsonNull.INSTANCE : new JsonPrimitive(expandRoles);
		object.add("expandRoles", jsonExpandRoles);
		JsonElement jsonRolesOnly = rolesOnly == null ? JsonNull.INSTANCE : new JsonPrimitive(rolesOnly);
		object.add("rolesOnly", jsonRolesOnly);
		JsonElement jsonPermissionsOnly = permissionsOnly == null ? JsonNull.INSTANCE : new JsonPrimitive(permissionsOnly);
		object.add("permissionsOnly", jsonPermissionsOnly);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("idsOnly")) {
			JsonElement jsonIdsOnly = jsonObject.get("idsOnly");
			if (jsonIdsOnly != null) {
				idsOnly = Boolean.valueOf(jsonIdsOnly.getAsBoolean());
			}
		}
		if (jsonObject.has("expandRoles")) {
			JsonElement jsonExpandRoles = jsonObject.get("expandRoles");
			if (jsonExpandRoles != null) {
				expandRoles = Boolean.valueOf(jsonExpandRoles.getAsBoolean());
			}
		}
		if (jsonObject.has("rolesOnly")) {
			JsonElement jsonRolesOnly = jsonObject.get("rolesOnly");
			if (jsonRolesOnly != null) {
				rolesOnly = Boolean.valueOf(jsonRolesOnly.getAsBoolean());
			}
		}
		if (jsonObject.has("permissionsOnly")) {
			JsonElement jsonPermissionsOnly = jsonObject.get("permissionsOnly");
			if (jsonPermissionsOnly != null) {
				permissionsOnly = Boolean.valueOf(jsonPermissionsOnly.getAsBoolean());
			}
		}
	}
}