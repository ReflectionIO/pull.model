//
//  Request.java
//  storedata
//
//  Created by William Shakour (billy1380) on 25 Nov 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.shared.datatypes;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

/**
 * @author billy1380
 * 
 */
public class Request extends com.willshex.gson.json.service.shared.Request {

	public Session session;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonSession = session == null ? JsonNull.INSTANCE : session.toJson();
		object.add("session", jsonSession);

		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("session")) {
			JsonElement jsonSession = jsonObject.get("session");
			if (jsonSession != null) {
				session = new Session();
				session.fromJson(jsonSession.getAsJsonObject());
			}
		}
	}
}
