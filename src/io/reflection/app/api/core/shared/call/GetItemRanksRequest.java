//  
//  GetItemRanksRequest.java
//  reflection.io
//
//  Created by William Shakour on October 15, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.Item;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class GetItemRanksRequest extends Request {
	public Item item;
	public Country country;
	public Category category;
	public Pager pager;
	public Date start;
	public Date end;
	public String listType;
	public String dailyData;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonItem = item == null ? JsonNull.INSTANCE : item.toJson();
		object.add("item", jsonItem);
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : country.toJson();
		object.add("country", jsonCountry);
		JsonElement jsonCategory = category == null ? JsonNull.INSTANCE : category.toJson();
		object.add("category", jsonCategory);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		JsonElement jsonStart = start == null ? JsonNull.INSTANCE : new JsonPrimitive(start.getTime());
		object.add("start", jsonStart);
		JsonElement jsonEnd = end == null ? JsonNull.INSTANCE : new JsonPrimitive(end.getTime());
		object.add("end", jsonEnd);
		JsonElement jsonListType = listType == null ? JsonNull.INSTANCE : new JsonPrimitive(listType);
		object.add("listType", jsonListType);
		JsonElement jsonDailyData = dailyData == null ? JsonNull.INSTANCE : new JsonPrimitive(dailyData);
		object.add("dailyData", jsonDailyData);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("item")) {
			JsonElement jsonItem = jsonObject.get("item");
			if (jsonItem != null) {
				item = new Item();
				item.fromJson(jsonItem.getAsJsonObject());
			}
		}
		if (jsonObject.has("country")) {
			JsonElement jsonCountry = jsonObject.get("country");
			if (jsonCountry != null) {
				country = new Country();
				country.fromJson(jsonCountry.getAsJsonObject());
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
		if (jsonObject.has("start")) {
			JsonElement jsonStart = jsonObject.get("start");
			if (jsonStart != null) {
				start = new Date(jsonStart.getAsLong());
			}
		}
		if (jsonObject.has("end")) {
			JsonElement jsonEnd = jsonObject.get("end");
			if (jsonEnd != null) {
				end = new Date(jsonEnd.getAsLong());
			}
		}
		if (jsonObject.has("listType")) {
			JsonElement jsonListType = jsonObject.get("listType");
			if (jsonListType != null) {
				listType = jsonListType.getAsString();
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