//  
//  GetAllTopItemsResponse.java
//  storedata
//
//  Created by William Shakour on October 2, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.Rank;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetAllTopItemsResponse extends Response {
	public List<Rank> freeRanks;
	public List<Rank> paidRanks;
	public List<Rank> grossingRanks;
	public List<Item> items;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonFreeRanks = JsonNull.INSTANCE;
		if (freeRanks != null) {
			jsonFreeRanks = new JsonArray();
			for (int i = 0; i < freeRanks.size(); i++) {
				JsonElement jsonFreeRanksItem = freeRanks.get(i) == null ? JsonNull.INSTANCE : freeRanks.get(i).toJson();
				((JsonArray) jsonFreeRanks).add(jsonFreeRanksItem);
			}
		}
		object.add("freeRanks", jsonFreeRanks);
		JsonElement jsonPaidRanks = JsonNull.INSTANCE;
		if (paidRanks != null) {
			jsonPaidRanks = new JsonArray();
			for (int i = 0; i < paidRanks.size(); i++) {
				JsonElement jsonPaidRanksItem = paidRanks.get(i) == null ? JsonNull.INSTANCE : paidRanks.get(i).toJson();
				((JsonArray) jsonPaidRanks).add(jsonPaidRanksItem);
			}
		}
		object.add("paidRanks", jsonPaidRanks);
		JsonElement jsonGrossingRanks = JsonNull.INSTANCE;
		if (grossingRanks != null) {
			jsonGrossingRanks = new JsonArray();
			for (int i = 0; i < grossingRanks.size(); i++) {
				JsonElement jsonGrossingRanksItem = grossingRanks.get(i) == null ? JsonNull.INSTANCE : grossingRanks.get(i).toJson();
				((JsonArray) jsonGrossingRanks).add(jsonGrossingRanksItem);
			}
		}
		object.add("grossingRanks", jsonGrossingRanks);
		JsonElement jsonItems = JsonNull.INSTANCE;
		if (items != null) {
			jsonItems = new JsonArray();
			for (int i = 0; i < items.size(); i++) {
				JsonElement jsonItemsItem = items.get(i) == null ? JsonNull.INSTANCE : items.get(i).toJson();
				((JsonArray) jsonItems).add(jsonItemsItem);
			}
		}
		object.add("items", jsonItems);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("freeRanks")) {
			JsonElement jsonFreeRanks = jsonObject.get("freeRanks");
			if (jsonFreeRanks != null) {
				freeRanks = new ArrayList<Rank>();
				Rank item = null;
				for (int i = 0; i < jsonFreeRanks.getAsJsonArray().size(); i++) {
					if (jsonFreeRanks.getAsJsonArray().get(i) != null) {
						(item = new Rank()).fromJson(jsonFreeRanks.getAsJsonArray().get(i).getAsJsonObject());
						freeRanks.add(item);
					}
				}
			}
		}

		if (jsonObject.has("paidRanks")) {
			JsonElement jsonPaidRanks = jsonObject.get("paidRanks");
			if (jsonPaidRanks != null) {
				paidRanks = new ArrayList<Rank>();
				Rank item = null;
				for (int i = 0; i < jsonPaidRanks.getAsJsonArray().size(); i++) {
					if (jsonPaidRanks.getAsJsonArray().get(i) != null) {
						(item = new Rank()).fromJson(jsonPaidRanks.getAsJsonArray().get(i).getAsJsonObject());
						paidRanks.add(item);
					}
				}
			}
		}

		if (jsonObject.has("grossingRanks")) {
			JsonElement jsonGrossingRanks = jsonObject.get("grossingRanks");
			if (jsonGrossingRanks != null) {
				grossingRanks = new ArrayList<Rank>();
				Rank item = null;
				for (int i = 0; i < jsonGrossingRanks.getAsJsonArray().size(); i++) {
					if (jsonGrossingRanks.getAsJsonArray().get(i) != null) {
						(item = new Rank()).fromJson(jsonGrossingRanks.getAsJsonArray().get(i).getAsJsonObject());
						grossingRanks.add(item);
					}
				}
			}
		}

		if (jsonObject.has("items")) {
			JsonElement jsonItems = jsonObject.get("items");
			if (jsonItems != null) {
				items = new ArrayList<Item>();
				Item item = null;
				for (int i = 0; i < jsonItems.getAsJsonArray().size(); i++) {
					if (jsonItems.getAsJsonArray().get(i) != null) {
						(item = new Item()).fromJson(jsonItems.getAsJsonArray().get(i).getAsJsonObject());
						items.add(item);
					}
				}
			}
		}

		if (jsonObject.has("pager")) {
			JsonElement jsonPager = jsonObject.get("pager");
			if (jsonPager != null) {
				pager = new Pager();
				pager.fromJson(jsonPager.getAsJsonObject());
			}
		}
	}
}