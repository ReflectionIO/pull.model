//  
//  GetLinkedAccountsResponse.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Response;
import io.reflection.app.datatypes.shared.DataAccount;
import io.reflection.app.datatypes.shared.DataSource;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetLinkedAccountsResponse extends Response {
	public List<DataAccount> linkedAccounts;
	public List<DataSource> dataSources;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonLinkedAccounts = JsonNull.INSTANCE;
		if (linkedAccounts != null) {
			jsonLinkedAccounts = new JsonArray();
			for (int i = 0; i < linkedAccounts.size(); i++) {
				JsonElement jsonLinkedAccountsItem = linkedAccounts.get(i) == null ? JsonNull.INSTANCE : linkedAccounts.get(i).toJson();
				((JsonArray) jsonLinkedAccounts).add(jsonLinkedAccountsItem);
			}
		}
		object.add("linkedAccounts", jsonLinkedAccounts);
		JsonElement jsonDataSources = JsonNull.INSTANCE;
		if (dataSources != null) {
			jsonDataSources = new JsonArray();
			for (int i = 0; i < dataSources.size(); i++) {
				JsonElement jsonDataSourcesItem = dataSources.get(i) == null ? JsonNull.INSTANCE : dataSources.get(i).toJson();
				((JsonArray) jsonDataSources).add(jsonDataSourcesItem);
			}
		}
		object.add("dataSources", jsonDataSources);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("linkedAccounts")) {
			JsonElement jsonLinkedAccounts = jsonObject.get("linkedAccounts");
			if (jsonLinkedAccounts != null) {
				linkedAccounts = new ArrayList<DataAccount>();
				DataAccount item = null;
				for (int i = 0; i < jsonLinkedAccounts.getAsJsonArray().size(); i++) {
					if (jsonLinkedAccounts.getAsJsonArray().get(i) != null) {
						(item = new DataAccount()).fromJson(jsonLinkedAccounts.getAsJsonArray().get(i).getAsJsonObject());
						linkedAccounts.add(item);
					}
				}
			}
		}

		if (jsonObject.has("dataSources")) {
			JsonElement jsonDataSources = jsonObject.get("dataSources");
			if (jsonDataSources != null) {
				dataSources = new ArrayList<DataSource>();
				DataSource item = null;
				for (int i = 0; i < jsonDataSources.getAsJsonArray().size(); i++) {
					if (jsonDataSources.getAsJsonArray().get(i) != null) {
						(item = new DataSource()).fromJson(jsonDataSources.getAsJsonArray().get(i).getAsJsonObject());
						dataSources.add(item);
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