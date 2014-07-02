//  
//  LinkAccountRequest.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.DataSource;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class LinkAccountRequest extends Request {
	public DataSource source;
	public String username;
	public String password;
	public String properties;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonSource = source == null ? JsonNull.INSTANCE : source.toJson();
		object.add("source", jsonSource);
		JsonElement jsonUsername = username == null ? JsonNull.INSTANCE : new JsonPrimitive(username);
		object.add("username", jsonUsername);
		JsonElement jsonPassword = password == null ? JsonNull.INSTANCE : new JsonPrimitive(password);
		object.add("password", jsonPassword);
		JsonElement jsonProperties = properties == null ? JsonNull.INSTANCE : new JsonPrimitive(properties);
		object.add("properties", jsonProperties);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("source")) {
			JsonElement jsonSource = jsonObject.get("source");
			if (jsonSource != null) {
				source = new DataSource();
				source.fromJson(jsonSource.getAsJsonObject());
			}
		}
		if (jsonObject.has("username")) {
			JsonElement jsonUsername = jsonObject.get("username");
			if (jsonUsername != null) {
				username = jsonUsername.getAsString();
			}
		}
		if (jsonObject.has("password")) {
			JsonElement jsonPassword = jsonObject.get("password");
			if (jsonPassword != null) {
				password = jsonPassword.getAsString();
			}
		}
		if (jsonObject.has("properties")) {
			JsonElement jsonProperties = jsonObject.get("properties");
			if (jsonProperties != null) {
				properties = jsonProperties.getAsString();
			}
		}
	}
}