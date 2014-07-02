//  
//  GetCountriesResponse.java
//  storedata
//
//  Created by William Shakour on 03 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Country;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetCountriesResponse extends Response {
	public List<Country> countries;
	public Pager pager;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCountries = JsonNull.INSTANCE;
		if (countries != null) {
			jsonCountries = new JsonArray();
			for (int i = 0; i < countries.size(); i++) {
				JsonElement jsonCountriesItem = countries.get(i) == null ? JsonNull.INSTANCE : countries.get(i).toJson();
				((JsonArray) jsonCountries).add(jsonCountriesItem);
			}
		}
		object.add("countries", jsonCountries);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("countries")) {
			JsonElement jsonCountries = jsonObject.get("countries");
			if (jsonCountries != null) {
				countries = new ArrayList<Country>();
				Country item = null;
				for (int i = 0; i < jsonCountries.getAsJsonArray().size(); i++) {
					if (jsonCountries.getAsJsonArray().get(i) != null) {
						(item = new Country()).fromJson(jsonCountries.getAsJsonArray().get(i).getAsJsonObject());
						countries.add(item);
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