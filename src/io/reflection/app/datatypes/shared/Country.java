//  
//  Country.java
//  storedata
//
//  Created by William Shakour on 21 Jun 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * @author billy1380
 * 
 *         Loosely based on http://en.wikipedia.org/wiki/ISO_3166-1
 */
public class Country extends DataType {

	/**
	 * Country name
	 */
	public String name;

	/**
	 * Country 2 character alpha code
	 */
	public String a2Code;

	/**
	 * Country 3 alpha character code
	 */
	public String a3Code;

	public String continent;

	/**
	 * Country numeric code
	 */
	public Integer nCode = Integer.valueOf(0);
	
	public List<String> stores;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonStores = JsonNull.INSTANCE;
		if (stores != null) {
			jsonStores = new JsonArray();
			for (int i = 0; i < stores.size(); i++) {
				JsonElement jsonStoresItem = stores.get(i) == null ? JsonNull.INSTANCE : new JsonPrimitive(stores.get(i));
				((JsonArray) jsonStores).add(jsonStoresItem);
			}
		}
		object.add("stores", jsonStores);
		JsonElement jsonName = name == null ? JsonNull.INSTANCE : new JsonPrimitive(name);
		object.add("name", jsonName);
		JsonElement jsonA2Code = a2Code == null ? JsonNull.INSTANCE : new JsonPrimitive(a2Code);
		object.add("a2Code", jsonA2Code);
		JsonElement jsonA3Code = a3Code == null ? JsonNull.INSTANCE : new JsonPrimitive(a3Code);
		object.add("a3Code", jsonA3Code);
		JsonElement jsonNCode = nCode == null ? JsonNull.INSTANCE : new JsonPrimitive(nCode);
		object.add("nCode", jsonNCode);
		JsonElement jsonContinent = continent == null ? JsonNull.INSTANCE : new JsonPrimitive(continent);
		object.add("continent", jsonContinent);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("stores")) {
			JsonElement jsonStores = jsonObject.get("stores");
			if (jsonStores != null) {
				stores = new ArrayList<String>();
				for (int i = 0; i < jsonStores.getAsJsonArray().size(); i++) {
					if (jsonStores.getAsJsonArray().get(i) != null) {
						stores.add(jsonStores.getAsJsonArray().get(i).getAsString());
					}
				}
			}
		}

		if (jsonObject.has("name")) {
			JsonElement jsonName = jsonObject.get("name");
			if (jsonName != null) {
				name = jsonName.getAsString();
			}
		}
		if (jsonObject.has("a2Code")) {
			JsonElement jsonA2Code = jsonObject.get("a2Code");
			if (jsonA2Code != null) {
				a2Code = jsonA2Code.getAsString();
			}
		}
		if (jsonObject.has("a3Code")) {
			JsonElement jsonA3Code = jsonObject.get("a3Code");
			if (jsonA3Code != null) {
				a3Code = jsonA3Code.getAsString();
			}
		}
		if (jsonObject.has("nCode")) {
			JsonElement jsonNCode = jsonObject.get("nCode");
			if (jsonNCode != null) {
				nCode = Integer.valueOf(jsonNCode.getAsInt());
			}
		}
		if (jsonObject.has("continent")) {
			JsonElement jsonContinent = jsonObject.get("continent");
			if (jsonContinent != null) {
				continent = jsonContinent.getAsString();
			}
		}
	}

}
