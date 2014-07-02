//  
//  SendEmailResponse.java
//  reflection.io
//
//  Created by William Shakour on February 20, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class SendEmailResponse extends Response {
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