//  
//  SimpleModelRun.java
//  reflection.io
//
//  Created by William Shakour on September 4, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SimpleModelRun extends DataType {
	public Category category;
	public String country;
	public String store;
	public Long code;
	public FormType form;
	public ListTypeType listType;
	public Double a;
	public Double b;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCategory = category == null ? JsonNull.INSTANCE : category.toJson();
		object.add("category", jsonCategory);
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : new JsonPrimitive(country);
		object.add("country", jsonCountry);
		JsonElement jsonStore = store == null ? JsonNull.INSTANCE : new JsonPrimitive(store);
		object.add("store", jsonStore);
		JsonElement jsonCode = code == null ? JsonNull.INSTANCE : new JsonPrimitive(code);
		object.add("code", jsonCode);
		JsonElement jsonForm = form == null ? JsonNull.INSTANCE : new JsonPrimitive(form.toString());
		object.add("form", jsonForm);
		JsonElement jsonListType = listType == null ? JsonNull.INSTANCE : new JsonPrimitive(listType.toString());
		object.add("listType", jsonListType);
		JsonElement jsonA = a == null ? JsonNull.INSTANCE : new JsonPrimitive(a);
		object.add("a", jsonA);
		JsonElement jsonB = b == null ? JsonNull.INSTANCE : new JsonPrimitive(b);
		object.add("b", jsonB);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("category")) {
			JsonElement jsonCategory = jsonObject.get("category");
			if (jsonCategory != null) {
				category = new Category();
				category.fromJson(jsonCategory.getAsJsonObject());
			}
		}
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
		if (jsonObject.has("listType")) {
			JsonElement jsonListType = jsonObject.get("listType");
			if (jsonListType != null) {
				listType = ListTypeType.fromString(jsonListType.getAsString());
			}
		}
		if (jsonObject.has("a")) {
			JsonElement jsonA = jsonObject.get("a");
			if (jsonA != null) {
				a = Double.valueOf(jsonA.getAsDouble());
			}
		}
		if (jsonObject.has("b")) {
			JsonElement jsonB = jsonObject.get("b");
			if (jsonB != null) {
				b = Double.valueOf(jsonB.getAsDouble());
			}
		}
	}
}