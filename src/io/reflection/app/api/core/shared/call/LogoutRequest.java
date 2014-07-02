//  
//  LogoutRequest.java
//  reflection.io
//
//  Created by William Shakour on November 25, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Request;

import com.google.gson.JsonObject;

public class LogoutRequest extends Request {
	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
	}
}