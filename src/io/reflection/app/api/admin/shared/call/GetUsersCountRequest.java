//  
//  GetUsersCountRequest.java
//  reflection.io
//
//  Created by William Shakour on October 11, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Request;

public class GetUsersCountRequest extends Request {
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