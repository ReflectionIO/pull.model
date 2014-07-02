//  
//  Resource.java
//  reflection.io
//
//  Created by William Shakour on February 17, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Resource extends DataType {
	public String name;
	public ResourceTypeType type;
	public String properties;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonName = name == null ? JsonNull.INSTANCE : new JsonPrimitive(name);
		object.add("name", jsonName);
		JsonElement jsonType = type == null ? JsonNull.INSTANCE : new JsonPrimitive(type.toString());
		object.add("type", jsonType);
		JsonElement jsonProperties = properties == null ? JsonNull.INSTANCE : new JsonPrimitive(properties);
		object.add("properties", jsonProperties);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("name")) {
			JsonElement jsonName = jsonObject.get("name");
			if (jsonName != null) {
				name = jsonName.getAsString();
			}
		}
		if (jsonObject.has("type")) {
			JsonElement jsonType = jsonObject.get("type");
			if (jsonType != null) {
				type = ResourceTypeType.fromString(jsonType.getAsString());
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