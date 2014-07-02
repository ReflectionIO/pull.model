//  
//  GetStoresResponse.java
//  storedata
//
//  Created by William Shakour on 03 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Store;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetStoresResponse extends Response {
	public List<Store> stores;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonStores = JsonNull.INSTANCE;
		if (stores != null) {
			jsonStores = new JsonArray();
			for (int i = 0; i < stores.size(); i++) {
				JsonElement jsonStoresItem = stores.get(i) == null ? JsonNull.INSTANCE : stores.get(i).toJson();
				((JsonArray) jsonStores).add(jsonStoresItem);
			}
		}
		object.add("stores", jsonStores);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("stores")) {
			JsonElement jsonStores = jsonObject.get("stores");
			if (jsonStores != null) {
				stores = new ArrayList<Store>();
				Store item = null;
				for (int i = 0; i < jsonStores.getAsJsonArray().size(); i++) {
					if (jsonStores.getAsJsonArray().get(i) != null) {
						(item = new Store()).fromJson(jsonStores.getAsJsonArray().get(i).getAsJsonObject());
						stores.add(item);
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