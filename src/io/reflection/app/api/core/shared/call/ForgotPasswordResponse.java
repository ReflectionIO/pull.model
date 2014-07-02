//  
//  ForgotPasswordResponse.java
//  reflection.io
//
//  Created by William Shakour on February 18, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class ForgotPasswordResponse extends Response {
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