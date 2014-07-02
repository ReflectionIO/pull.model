//  
//  GetCategoriesResponse.java
//  reflection.io
//
//  Created by William Shakour on March 11, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Category;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetCategoriesResponse extends Response {
	public List<Category> categories;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCategories = JsonNull.INSTANCE;
		if (categories != null) {
			jsonCategories = new JsonArray();
			for (int i = 0; i < categories.size(); i++) {
				JsonElement jsonCategoriesItem = categories.get(i) == null ? JsonNull.INSTANCE : categories.get(i).toJson();
				((JsonArray) jsonCategories).add(jsonCategoriesItem);
			}
		}
		object.add("categories", jsonCategories);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("categories")) {
			JsonElement jsonCategories = jsonObject.get("categories");
			if (jsonCategories != null) {
				categories = new ArrayList<Category>();
				Category item = null;
				for (int i = 0; i < jsonCategories.getAsJsonArray().size(); i++) {
					if (jsonCategories.getAsJsonArray().get(i) != null) {
						(item = new Category()).fromJson(jsonCategories.getAsJsonArray().get(i).getAsJsonObject());
						categories.add(item);
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