//  
//  GetLinkedAccountItemsRequest.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.DataAccount;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetLinkedAccountItemsRequest extends Request {
	public DataAccount linkedAccount;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonLinkedAccount = linkedAccount == null ? JsonNull.INSTANCE : linkedAccount.toJson();
		object.add("linkedAccount", jsonLinkedAccount);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
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
		if (jsonObject.has("pager")) {
			JsonElement jsonPager = jsonObject.get("pager");
			if (jsonPager != null) {
				pager = new Pager();
				pager.fromJson(jsonPager.getAsJsonObject());
			}
		}
	}
}