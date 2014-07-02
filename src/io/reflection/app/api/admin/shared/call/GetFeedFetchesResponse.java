//  
//  GetFeedFetchesResponse.java
//  reflection.io
//
//  Created by William Shakour on October 22, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.FeedFetch;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetFeedFetchesResponse extends Response {
	public List<FeedFetch> feedFetches;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonFeedFetches = JsonNull.INSTANCE;
		if (feedFetches != null) {
			jsonFeedFetches = new JsonArray();
			for (int i = 0; i < feedFetches.size(); i++) {
				JsonElement jsonFeedFetchesItem = feedFetches.get(i) == null ? JsonNull.INSTANCE : feedFetches.get(i).toJson();
				((JsonArray) jsonFeedFetches).add(jsonFeedFetchesItem);
			}
		}
		object.add("feedFetches", jsonFeedFetches);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("feedFetches")) {
			JsonElement jsonFeedFetches = jsonObject.get("feedFetches");
			if (jsonFeedFetches != null) {
				feedFetches = new ArrayList<FeedFetch>();
				FeedFetch item = null;
				for (int i = 0; i < jsonFeedFetches.getAsJsonArray().size(); i++) {
					if (jsonFeedFetches.getAsJsonArray().get(i) != null) {
						(item = new FeedFetch()).fromJson(jsonFeedFetches.getAsJsonArray().get(i).getAsJsonObject());
						feedFetches.add(item);
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