//
//  UpdateLinkedAccountRequest.java
//  storedata
//
//  Created by Stefano Capuzzi on 14 Apr 2014.
//  Copyright �� 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.DataAccount;

/**
 * @author stefanocapuzzi
 *
 */
public class UpdateLinkedAccountRequest  extends Request {
	public DataAccount linkedAccount;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonLinkedAccount = linkedAccount == null ? JsonNull.INSTANCE : linkedAccount.toJson();
		object.add("linkedAccount", jsonLinkedAccount);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("linkedAccount")) {
			JsonElement jsonLinkedAccount = jsonObject.get("linkedAccount");
			if (jsonLinkedAccount != null) {
				linkedAccount = new DataAccount();
				linkedAccount.fromJson(jsonLinkedAccount.getAsJsonObject());
			}
		}
	}
}
