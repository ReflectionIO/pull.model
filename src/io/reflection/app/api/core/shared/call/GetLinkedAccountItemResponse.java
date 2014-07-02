//  
//  GetLinkedAccountItemResponse.java
//  reflection.io
//
//  Created by William Shakour on May 19, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Response;
import io.reflection.app.datatypes.shared.DataAccount;
import io.reflection.app.datatypes.shared.DataSource;
import io.reflection.app.datatypes.shared.Item;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetLinkedAccountItemResponse extends Response {
	public Item item;
	public DataAccount linkedAccount;
	public DataSource dataSource;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonItem = item == null ? JsonNull.INSTANCE : item.toJson();
		object.add("item", jsonItem);
		JsonElement jsonLinkedAccount = linkedAccount == null ? JsonNull.INSTANCE : linkedAccount.toJson();
		object.add("linkedAccount", jsonLinkedAccount);
		JsonElement jsonDataSource = dataSource == null ? JsonNull.INSTANCE : dataSource.toJson();
		object.add("dataSource", jsonDataSource);
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