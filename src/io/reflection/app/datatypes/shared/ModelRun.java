//  
//  ModelRun.java
//  reflection.io
//
//  Created by William Shakour on October 28, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ModelRun extends DataType {
	public String country;
	public String store;
	public Long code;
	public FormType form;
	public Double grossingA;
	public Double paidA;
	public Double bRatio;
	public Double totalDownloads;
	public Double paidB;
	public Double grossingB;
	public Double paidAIap;
	public Double grossingAIap;
	public Double freeA;
	public Double theta;
	public Double freeB;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : new JsonPrimitive(country);
		object.add("country", jsonCountry);
		JsonElement jsonStore = store == null ? JsonNull.INSTANCE : new JsonPrimitive(store);
		object.add("store", jsonStore);
		JsonElement jsonCode = code == null ? JsonNull.INSTANCE : new JsonPrimitive(code);
		object.add("code", jsonCode);
		JsonElement jsonForm = form == null ? JsonNull.INSTANCE : new JsonPrimitive(form.toString());
		object.add("form", jsonForm);
		JsonElement jsonGrossingA = grossingA == null ? JsonNull.INSTANCE : new JsonPrimitive(grossingA);
		object.add("grossingA", jsonGrossingA);
		JsonElement jsonPaidA = paidA == null ? JsonNull.INSTANCE : new JsonPrimitive(paidA);
		object.add("paidA", jsonPaidA);
		JsonElement jsonBRatio = bRatio == null ? JsonNull.INSTANCE : new JsonPrimitive(bRatio);
		object.add("bRatio", jsonBRatio);
		JsonElement jsonTotalDownloads = totalDownloads == null ? JsonNull.INSTANCE : new JsonPrimitive(totalDownloads);
		object.add("totalDownloads", jsonTotalDownloads);
		JsonElement jsonPaidB = paidB == null ? JsonNull.INSTANCE : new JsonPrimitive(paidB);
		object.add("paidB", jsonPaidB);
		JsonElement jsonGrossingB = grossingB == null ? JsonNull.INSTANCE : new JsonPrimitive(grossingB);
		object.add("grossingB", jsonGrossingB);
		JsonElement jsonPaidAIap = paidAIap == null ? JsonNull.INSTANCE : new JsonPrimitive(paidAIap);
		object.add("paidAIap", jsonPaidAIap);
		JsonElement jsonGrossingAIap = grossingAIap == null ? JsonNull.INSTANCE : new JsonPrimitive(grossingAIap);
		object.add("grossingAIap", jsonGrossingAIap);
		JsonElement jsonFreeA = freeA == null ? JsonNull.INSTANCE : new JsonPrimitive(freeA);
		object.add("freeA", jsonFreeA);
		JsonElement jsonTheta = theta == null ? JsonNull.INSTANCE : new JsonPrimitive(theta);
		object.add("theta", jsonTheta);
		JsonElement jsonFreeB = freeB == null ? JsonNull.INSTANCE : new JsonPrimitive(freeB);
		object.add("freeB", jsonFreeB);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("country")) {
			JsonElement jsonCountry = jsonObject.get("country");
			if (jsonCountry != null) {
				country = jsonCountry.getAsString();
			}
		}
		if (jsonObject.has("store")) {
			JsonElement jsonStore = jsonObject.get("store");
			if (jsonStore != null) {
				store = jsonStore.getAsString();
			}
		}
		if (jsonObject.has("code")) {
			JsonElement jsonCode = jsonObject.get("code");
			if (jsonCode != null) {
				code = Long.valueOf(jsonCode.getAsLong());
			}
		}
		if (jsonObject.has("form")) {
			JsonElement jsonForm = jsonObject.get("form");
			if (jsonForm != null) {
				form = FormType.fromString(jsonForm.getAsString());
			}
		}
		if (jsonObject.has("grossingA")) {
			JsonElement jsonGrossingA = jsonObject.get("grossingA");
			if (jsonGrossingA != null) {
				grossingA = Double.valueOf(jsonGrossingA.getAsDouble());
			}
		}
		if (jsonObject.has("paidA")) {
			JsonElement jsonPaidA = jsonObject.get("paidA");
			if (jsonPaidA != null) {
				paidA = Double.valueOf(jsonPaidA.getAsDouble());
			}
		}
		if (jsonObject.has("bRatio")) {
			JsonElement jsonBRatio = jsonObject.get("bRatio");
			if (jsonBRatio != null) {
				bRatio = Double.valueOf(jsonBRatio.getAsDouble());
			}
		}
		if (jsonObject.has("totalDownloads")) {
			JsonElement jsonTotalDownloads = jsonObject.get("totalDownloads");
			if (jsonTotalDownloads != null) {
				totalDownloads = Double.valueOf(jsonTotalDownloads.getAsDouble());
			}
		}
		if (jsonObject.has("paidB")) {
			JsonElement jsonPaidB = jsonObject.get("paidB");
			if (jsonPaidB != null) {
				paidB = Double.valueOf(jsonPaidB.getAsDouble());
			}
		}
		if (jsonObject.has("grossingB")) {
			JsonElement jsonGrossingB = jsonObject.get("grossingB");
			if (jsonGrossingB != null) {
				grossingB = Double.valueOf(jsonGrossingB.getAsDouble());
			}
		}
		if (jsonObject.has("paidAIap")) {
			JsonElement jsonPaidAIap = jsonObject.get("paidAIap");
			if (jsonPaidAIap != null) {
				paidAIap = Double.valueOf(jsonPaidAIap.getAsDouble());
			}
		}
		if (jsonObject.has("grossingAIap")) {
			JsonElement jsonGrossingAIap = jsonObject.get("grossingAIap");
			if (jsonGrossingAIap != null) {
				grossingAIap = Double.valueOf(jsonGrossingAIap.getAsDouble());
			}
		}
		if (jsonObject.has("freeA")) {
			JsonElement jsonFreeA = jsonObject.get("freeA");
			if (jsonFreeA != null) {
				freeA = Double.valueOf(jsonFreeA.getAsDouble());
			}
		}
		if (jsonObject.has("theta")) {
			JsonElement jsonTheta = jsonObject.get("theta");
			if (jsonTheta != null) {
				theta = Double.valueOf(jsonTheta.getAsDouble());
			}
		}
		if (jsonObject.has("freeB")) {
			JsonElement jsonFreeB = jsonObject.get("freeB");
			if (jsonFreeB != null) {
				freeB = Double.valueOf(jsonFreeB.getAsDouble());
			}
		}
	}
}