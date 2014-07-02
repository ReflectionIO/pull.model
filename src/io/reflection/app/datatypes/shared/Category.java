//  
//  Category.java
//  reflection.io
//
//  Created by William Shakour on January 31, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Category extends DataType {
	public Category parent;
	public Long internalId;
	public String name;
	public String store;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonParent = parent == null ? JsonNull.INSTANCE : parent.toJson();
		object.add("parent", jsonParent);
		JsonElement jsonInternalId = internalId == null ? JsonNull.INSTANCE : new JsonPrimitive(internalId);
		object.add("internalId", jsonInternalId);
		JsonElement jsonName = name == null ? JsonNull.INSTANCE : new JsonPrimitive(name);
		object.add("name", jsonName);
		JsonElement jsonStore = store == null ? JsonNull.INSTANCE : new JsonPrimitive(store);
		object.add("store", jsonStore);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("parent")) {
			JsonElement jsonParent = jsonObject.get("parent");
			if (jsonParent != null) {
				parent = new Category();
				parent.fromJson(jsonParent.getAsJsonObject());
			}
		}
		if (jsonObject.has("internalId")) {
			JsonElement jsonInternalId = jsonObject.get("internalId");
			if (jsonInternalId != null) {
				internalId = Long.valueOf(jsonInternalId.getAsLong());
			}
		}
		if (jsonObject.has("name")) {
			JsonElement jsonName = jsonObject.get("name");
			if (jsonName != null) {
				name = jsonName.getAsString();
			}
		}
		if (jsonObject.has("store")) {
			JsonElement jsonStore = jsonObject.get("store");
			if (jsonStore != null) {
				store = jsonStore.getAsString();
			}
		}
	}
}