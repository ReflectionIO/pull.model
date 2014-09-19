//  
//  GetModelOutcomeResponse.java
//  reflection.io
//
//  Created by William Shakour on October 18, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Response;
import io.reflection.app.datatypes.shared.ModelRun;
import io.reflection.app.datatypes.shared.SimpleModelRun;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetModelOutcomeResponse extends Response {
	public ModelRun correlation;
	public SimpleModelRun simple;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCorrelation = correlation == null ? JsonNull.INSTANCE : correlation.toJson();
		object.add("correlation", jsonCorrelation);
		JsonElement jsonSimple = simple == null ? JsonNull.INSTANCE : simple.toJson();
		object.add("simple", jsonSimple);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("correlation")) {
			JsonElement jsonCorrelation = jsonObject.get("correlation");
			if (jsonCorrelation != null) {
				correlation = new ModelRun();
				correlation.fromJson(jsonCorrelation.getAsJsonObject());
			}
		}
		if (jsonObject.has("simple")) {
			JsonElement jsonSimple = jsonObject.get("simple");
			if (jsonSimple != null) {
				simple = new SimpleModelRun();
				simple.fromJson(jsonSimple.getAsJsonObject());
			}
		}
	}
}