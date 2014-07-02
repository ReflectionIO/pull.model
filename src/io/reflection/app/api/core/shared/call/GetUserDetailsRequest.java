//  
//  GetUserDetailsRequest.java
//  reflection.io
//
//  Created by William Shakour on March 7, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Request;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class GetUserDetailsRequest extends Request {
	public Long userId;
	public String actionCode;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonUserId = userId == null ? JsonNull.INSTANCE : new JsonPrimitive(userId);
		object.add("userId", jsonUserId);
		JsonElement jsonActionCode = actionCode == null ? JsonNull.INSTANCE : new JsonPrimitive(actionCode);
		object.add("actionCode", jsonActionCode);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("userId")) {
			JsonElement jsonUserId = jsonObject.get("userId");
			if (jsonUserId != null) {
				userId = Long.valueOf(jsonUserId.getAsLong());
			}
		}
		if (jsonObject.has("actionCode")) {
			JsonElement jsonActionCode = jsonObject.get("actionCode");
			if (jsonActionCode != null) {
				actionCode = jsonActionCode.getAsString();
			}
		}
	}
}