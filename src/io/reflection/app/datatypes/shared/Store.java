//  
//  Store.java
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
 */
public class Store extends DataType {

	/**
	 * Store 3 character alpha code
	 */
	public String a3Code;

	/**
	 * Store name
	 */
	public String name;

	/**
	 * Store online url
	 */
	public String url;

	public List<String> countries;
	public String datasource;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCountries = JsonNull.INSTANCE;
		if (countries != null) {
			jsonCountries = new JsonArray();
			for (int i = 0; i < countries.size(); i++) {
				JsonElement jsonCountriesItem = countries.get(i) == null ? JsonNull.INSTANCE : new JsonPrimitive(countries.get(i));
				((JsonArray) jsonCountries).add(jsonCountriesItem);
			}
		}
		object.add("countries", jsonCountries);
		JsonElement jsonA3Code = a3Code == null ? JsonNull.INSTANCE : new JsonPrimitive(a3Code);
		object.add("a3Code", jsonA3Code);
		JsonElement jsonName = name == null ? JsonNull.INSTANCE : new JsonPrimitive(name);
		object.add("name", jsonName);
		JsonElement jsonUrl = url == null ? JsonNull.INSTANCE : new JsonPrimitive(url);
		object.add("url", jsonUrl);
		JsonElement jsonDatasource = datasource == null ? JsonNull.INSTANCE : new JsonPrimitive(datasource);
		object.add("datasource", jsonDatasource);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("countries")) {
			JsonElement jsonCountries = jsonObject.get("countries");
			if (jsonCountries != null) {
				countries = new ArrayList<String>();
				String item = null;
				for (int i = 0; i < jsonCountries.getAsJsonArray().size(); i++) {
					if (jsonCountries.getAsJsonArray().get(i) != null) {
						item = jsonCountries.getAsJsonArray().get(i).getAsString();
						countries.add(item);
					}
				}
			}
		}

		if (jsonObject.has("a3Code")) {
			JsonElement jsonA3Code = jsonObject.get("a3Code");
			if (jsonA3Code != null) {
				a3Code = jsonA3Code.getAsString();
			}
		}
		if (jsonObject.has("name")) {
			JsonElement jsonName = jsonObject.get("name");
			if (jsonName != null) {
				name = jsonName.getAsString();
			}
		}
		if (jsonObject.has("url")) {
			JsonElement jsonUrl = jsonObject.get("url");
			if (jsonUrl != null) {
				url = jsonUrl.getAsString();
			}
		}
		if (jsonObject.has("datasource")) {
			JsonElement jsonDatasource = jsonObject.get("datasource");
			if (jsonDatasource != null) {
				datasource = jsonDatasource.getAsString();
			}
		}
	}
}