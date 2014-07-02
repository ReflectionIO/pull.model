//  
//  LinkAccountResponse.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.datatypes.shared.DataAccount;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class LinkAccountResponse extends Response {
	public DataAccount account;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonAccount = account == null ? JsonNull.INSTANCE : account.toJson();
		object.add("account", jsonAccount);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("account")) {
			JsonElement jsonAccount = jsonObject.get("account");
			if (jsonAccount != null) {
				account = new DataAccount();
				account.fromJson(jsonAccount.getAsJsonObject());
			}
		}
	}
}