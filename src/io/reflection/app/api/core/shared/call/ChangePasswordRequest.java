//  
//  ChangePasswordRequest.java
//  reflection.io
//
//  Created by William Shakour on December 16, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Request;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ChangePasswordRequest extends Request {
	public String resetCode;
	public String password;
	public String newPassword;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonResetCode = resetCode == null ? JsonNull.INSTANCE : new JsonPrimitive(resetCode);
		object.add("resetCode", jsonResetCode);
		JsonElement jsonPassword = password == null ? JsonNull.INSTANCE : new JsonPrimitive(password);
		object.add("password", jsonPassword);
		JsonElement jsonNewPassword = newPassword == null ? JsonNull.INSTANCE : new JsonPrimitive(newPassword);
		object.add("newPassword", jsonNewPassword);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("resetCode")) {
			JsonElement jsonResetCode = jsonObject.get("resetCode");
			if (jsonResetCode != null) {
				resetCode = jsonResetCode.getAsString();
			}
		}
		if (jsonObject.has("password")) {
			JsonElement jsonPassword = jsonObject.get("password");
			if (jsonPassword != null) {
				password = jsonPassword.getAsString();
			}
		}
		if (jsonObject.has("newPassword")) {
			JsonElement jsonNewPassword = jsonObject.get("newPassword");
			if (jsonNewPassword != null) {
				newPassword = jsonNewPassword.getAsString();
			}
		}
	}
}