//  
//  DataAccountFetch.java
//  reflection.io
//
//  Created by William Shakour on January 14, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DataAccountFetch extends DataType {
	public DataAccount linkedAccount;
	public DataAccountFetchStatusType status;
	public String data;
	public Date date;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonLinkedAccount = linkedAccount == null ? JsonNull.INSTANCE : linkedAccount.toJson();
		object.add("linkedAccount", jsonLinkedAccount);
		JsonElement jsonStatus = status == null ? JsonNull.INSTANCE : new JsonPrimitive(status.toString());
		object.add("status", jsonStatus);
		JsonElement jsonData = data == null ? JsonNull.INSTANCE : new JsonPrimitive(data);
		object.add("data", jsonData);
		JsonElement jsonDate = date == null ? JsonNull.INSTANCE : new JsonPrimitive(date.getTime());
		object.add("date", jsonDate);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("linkedAccount")) {
			JsonElement jsonLinkedAccount = jsonObject.get("linkedAccount");
			if (jsonLinkedAccount != null) {
				linkedAccount = new DataAccount();
				linkedAccount.fromJson(jsonLinkedAccount.getAsJsonObject());
			}
		}
		if (jsonObject.has("status")) {
			JsonElement jsonStatus = jsonObject.get("status");
			if (jsonStatus != null) {
				status = DataAccountFetchStatusType.fromString(jsonStatus.getAsString());
			}
		}
		if (jsonObject.has("data")) {
			JsonElement jsonData = jsonObject.get("data");
			if (jsonData != null) {
				data = jsonData.getAsString();
			}
		}
		if (jsonObject.has("date")) {
			JsonElement jsonDate = jsonObject.get("date");
			if (jsonDate != null) {
				date = new Date(jsonDate.getAsLong());
			}
		}
	}
}