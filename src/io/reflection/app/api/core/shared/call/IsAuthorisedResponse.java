//  
//  IsAuthorisedResponse.java
//  reflection.io
//
//  Created by William Shakour on January 16, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.reflection.app.api.shared.datatypes.Response;

public class IsAuthorisedResponse extends Response {
	public Boolean authorised;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonAuthorised = authorised == null ? JsonNull.INSTANCE : new JsonPrimitive(authorised);
		object.add("authorised", jsonAuthorised);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("authorised")) {
			JsonElement jsonAuthorised = jsonObject.get("authorised");
			if (jsonAuthorised != null) {
				authorised = Boolean.valueOf(jsonAuthorised.getAsBoolean());
			}
		}
	}
}