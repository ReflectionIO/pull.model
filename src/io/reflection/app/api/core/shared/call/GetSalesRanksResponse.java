//  
//  GetSalesRanksResponse.java
//  reflection.io
//
//  Created by William Shakour on April 20, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Response;
import io.reflection.app.datatypes.shared.DataAccount;
import io.reflection.app.datatypes.shared.DataSource;
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.Rank;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetSalesRanksResponse extends Response {
	public List<Rank> ranks;
	public Pager pager;
	public List<Item> items;
	public DataAccount linkedAccount;
	public DataSource dataSource;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonRanks = JsonNull.INSTANCE;
		if (ranks != null) {
			jsonRanks = new JsonArray();
			for (int i = 0; i < ranks.size(); i++) {
				JsonElement jsonRanksItem = ranks.get(i) == null ? JsonNull.INSTANCE : ranks.get(i).toJson();
				((JsonArray) jsonRanks).add(jsonRanksItem);
			}
		}
		object.add("ranks", jsonRanks);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		JsonElement jsonItems = JsonNull.INSTANCE;
		if (items != null) {
			jsonItems = new JsonArray();
			for (int i = 0; i < items.size(); i++) {
				JsonElement jsonItemsItem = items.get(i) == null ? JsonNull.INSTANCE : items.get(i).toJson();
				((JsonArray) jsonItems).add(jsonItemsItem);
			}
		}
		object.add("items", jsonItems);
		JsonElement jsonLinkedAccount = linkedAccount == null ? JsonNull.INSTANCE : linkedAccount.toJson();
		object.add("linkedAccount", jsonLinkedAccount);
		JsonElement jsonDataSource = dataSource == null ? JsonNull.INSTANCE : dataSource.toJson();
		object.add("dataSource", jsonDataSource);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("ranks")) {
			JsonElement jsonRanks = jsonObject.get("ranks");
			if (jsonRanks != null) {
				ranks = new ArrayList<Rank>();
				Rank item = null;
				for (int i = 0; i < jsonRanks.getAsJsonArray().size(); i++) {
					if (jsonRanks.getAsJsonArray().get(i) != null) {
						(item = new Rank()).fromJson(jsonRanks.getAsJsonArray().get(i).getAsJsonObject());
						ranks.add(item);
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

		if (jsonObject.has("linkedAccount")) {
			JsonElement jsonLinkedAccount = jsonObject.get("linkedAccount");
			if (jsonLinkedAccount != null) {
				linkedAccount = new DataAccount();
				linkedAccount.fromJson(jsonLinkedAccount.getAsJsonObject());
			}
		}
		if (jsonObject.has("dataSource")) {
			JsonElement jsonDataSource = jsonObject.get("dataSource");
			if (jsonDataSource != null) {
				dataSource = new DataSource();
				dataSource.fromJson(jsonDataSource.getAsJsonObject());
			}
		}
	}
}