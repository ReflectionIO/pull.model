//  
//  GetModelOutcomeResponse.java
//  reflection.io
//
//  Created by William Shakour on October 18, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.datatypes.shared.ModelRun;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetModelOutcomeResponse extends Response {
	public ModelRun run;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonRun = run == null ? JsonNull.INSTANCE : run.toJson();
		object.add("run", jsonRun);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("run")) {
			JsonElement jsonRun = jsonObject.get("run");
			if (jsonRun != null) {
				run = new ModelRun();
				run.fromJson(jsonRun.getAsJsonObject());
			}
		}
	}
}