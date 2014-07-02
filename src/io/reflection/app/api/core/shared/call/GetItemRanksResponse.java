//  
//  GetItemRanksResponse.java
//  storedata
//
//  Created by William Shakour on September 10, 2013.
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

public class GetItemRanksResponse extends Response {
	public List<Rank> ranks;
	public Pager pager;
	public Item item;

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
		JsonElement jsonItem = item == null ? JsonNull.INSTANCE : item.toJson();
		object.add("item", jsonItem);
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
		if (jsonObject.has("item")) {
			JsonElement jsonItem = jsonObject.get("item");
			if (jsonItem != null) {
				item = new Item();
				item.fromJson(jsonItem.getAsJsonObject());
			}
		}
	}
}