//  
//  GetTopItemsRequest.java
//  storedata
//
//  Created by William Shakour on 03 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.Store;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class GetTopItemsRequest extends Request {
	public Country country;
	public Store store;
	public Category category;
	public Pager pager;
	public String listType;
	public Date on;
	public String dailyData;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : country.toJson();
		object.add("country", jsonCountry);
		JsonElement jsonStore = store == null ? JsonNull.INSTANCE : store.toJson();
		object.add("store", jsonStore);
		JsonElement jsonCategory = category == null ? JsonNull.INSTANCE : category.toJson();
		object.add("category", jsonCategory);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		JsonElement jsonListType = listType == null ? JsonNull.INSTANCE : new JsonPrimitive(listType);
		object.add("listType", jsonListType);
		JsonElement jsonOn = on == null ? JsonNull.INSTANCE : new JsonPrimitive(on.getTime());
		object.add("on", jsonOn);
		JsonElement jsonDailyData = dailyData == null ? JsonNull.INSTANCE : new JsonPrimitive(dailyData);
		object.add("dailyData", jsonDailyData);
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
		if (jsonObject.has("category")) {
			JsonElement jsonCategory = jsonObject.get("category");
			if (jsonCategory != null) {
				category = new Category();
				category.fromJson(jsonCategory.getAsJsonObject());
			}
		}
		if (jsonObject.has("pager")) {
			JsonElement jsonPager = jsonObject.get("pager");
			if (jsonPager != null) {
				pager = new Pager();
				pager.fromJson(jsonPager.getAsJsonObject());
			}
		}
		if (jsonObject.has("listType")) {
			JsonElement jsonListType = jsonObject.get("listType");
			if (jsonListType != null) {
				listType = jsonListType.getAsString();
			}
		}
		if (jsonObject.has("on")) {
			JsonElement jsonOn = jsonObject.get("on");
			if (jsonOn != null) {
				on = new Date(jsonOn.getAsLong());
			}
		}
		if (jsonObject.has("dailyData")) {
			JsonElement jsonDailyData = jsonObject.get("dailyData");
			if (jsonDailyData != null) {
				dailyData = jsonDailyData.getAsString();
			}
		}
	}
}