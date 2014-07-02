//  
//  GetFeedFetchesRequest.java
//  reflection.io
//
//  Created by William Shakour on October 22, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.Store;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;


public class GetFeedFetchesRequest extends Request {
	public Country country;
	public Store store;
	public Pager pager;
	public List<String> listTypes;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : country.toJson();
		object.add("country", jsonCountry);
		JsonElement jsonStore = store == null ? JsonNull.INSTANCE : store.toJson();
		object.add("store", jsonStore);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		JsonElement jsonListTypes = JsonNull.INSTANCE;
		if (listTypes != null) {
			jsonListTypes = new JsonArray();
			for (int i = 0; i < listTypes.size(); i++) {
				JsonElement jsonListTypesItem = listTypes.get(i) == null ? JsonNull.INSTANCE : new JsonPrimitive(listTypes.get(i));
				((JsonArray) jsonListTypes).add(jsonListTypesItem);
			}
		}
		object.add("listTypes", jsonListTypes);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("country")) {
			JsonElement jsonCountry = jsonObject.get("country");
			if (jsonCountry != null) {
				country = new Country();
				country.fromJson(jsonCountry.getAsJsonObject());
			}
		}
		if (jsonObject.has("store")) {
			JsonElement jsonStore = jsonObject.get("store");
			if (jsonStore != null) {
				store = new Store();
				store.fromJson(jsonStore.getAsJsonObject());
			}
		}
		if (jsonObject.has("pager")) {
			JsonElement jsonPager = jsonObject.get("pager");
			if (jsonPager != null) {
				pager = new Pager();
				pager.fromJson(jsonPager.getAsJsonObject());
			}
		}
		if (jsonObject.has("listTypes")) {
			JsonElement jsonListTypes = jsonObject.get("listTypes");
			if (jsonListTypes != null) {
				listTypes = new ArrayList<String>();
				String item = null;
				for (int i = 0; i < jsonListTypes.getAsJsonArray().size(); i++) {
					if (jsonListTypes.getAsJsonArray().get(i) != null) {
						item = jsonListTypes.getAsJsonArray().get(i).getAsString();
						listTypes.add(item);
					}
				}
			}
		}

	}
}