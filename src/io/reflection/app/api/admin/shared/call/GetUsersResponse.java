//  
//  GetUsersResponse.java
//  reflection.io
//
//  Created by William Shakour on October 9, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.User;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetUsersResponse extends Response {
	public List<User> users;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonUsers = JsonNull.INSTANCE;
		if (users != null) {
			jsonUsers = new JsonArray();
			for (int i = 0; i < users.size(); i++) {
				JsonElement jsonUsersItem = users.get(i) == null ? JsonNull.INSTANCE : users.get(i).toJson();
				((JsonArray) jsonUsers).add(jsonUsersItem);
			}
		}
		object.add("users", jsonUsers);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("users")) {
			JsonElement jsonUsers = jsonObject.get("users");
			if (jsonUsers != null) {
				users = new ArrayList<User>();
				User item = null;
				for (int i = 0; i < jsonUsers.getAsJsonArray().size(); i++) {
					if (jsonUsers.getAsJsonArray().get(i) != null) {
						(item = new User()).fromJson(jsonUsers.getAsJsonArray().get(i).getAsJsonObject());
						users.add(item);
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