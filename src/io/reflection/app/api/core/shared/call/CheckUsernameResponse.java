//  
//  CheckUsernameResponse.java
//  reflection.io
//
//  Created by William Shakour on November 25, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.reflection.app.api.shared.datatypes.Response;

public class CheckUsernameResponse extends Response {
	public Boolean usernameInUse;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonUsernameInUse = usernameInUse == null ? JsonNull.INSTANCE : new JsonPrimitive(usernameInUse);
		object.add("usernameInUse", jsonUsernameInUse);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("usernameInUse")) {
			JsonElement jsonUsernameInUse = jsonObject.get("usernameInUse");
			if (jsonUsernameInUse != null) {
				usernameInUse = Boolean.valueOf(jsonUsernameInUse.getAsBoolean());
			}
		}
	}
}