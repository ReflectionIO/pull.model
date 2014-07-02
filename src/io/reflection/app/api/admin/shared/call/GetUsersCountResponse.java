//  
//  GetUsersCountResponse.java
//  reflection.io
//
//  Created by William Shakour on October 11, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.reflection.app.api.shared.datatypes.Response;

public class GetUsersCountResponse extends Response {
	public Long count;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCount = count == null ? JsonNull.INSTANCE : new JsonPrimitive(count);
		object.add("count", jsonCount);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("count")) {
			JsonElement jsonCount = jsonObject.get("count");
			if (jsonCount != null) {
				count = Long.valueOf(jsonCount.getAsLong());
			}
		}
	}
}