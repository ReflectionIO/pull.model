//  
//  GetSalesResponse.java
//  reflection.io
//
//  Created by William Shakour on April 16, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.Response;
import io.reflection.app.datatypes.shared.DataSource;
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.Sale;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

public class GetSalesResponse extends Response {
	
	public List<Sale> sales;
	public Pager pager;
	public List<Item> items;
	public DataSource dataSource;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonSales = JsonNull.INSTANCE;
		if (sales != null) {
			jsonSales = new JsonArray();
			for (int i = 0; i < sales.size(); i++) {
				JsonElement jsonSalesItem = sales.get(i) == null ? JsonNull.INSTANCE : sales.get(i).toJson();
				((JsonArray) jsonSales).add(jsonSalesItem);
			}
		}
		object.add("sales", jsonSales);
		JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
		object.add("pager", jsonPager);
		JsonElement jsonItems = JsonNull.INSTANCE;
		if (items != null) {
			jsonItems = new JsonArray();
			for (int i = 0; i < items.size(); i++) {
				JsonElement jsonItemsItem = items.get(i) == null ? JsonNull.INSTANCE : items.get(i).toJson();
				((JsonArray) jsonItems).add(jsonItemsItem);
			}
		}
		object.add("items", jsonItems);
		JsonElement jsonDataSource = dataSource == null ? JsonNull.INSTANCE : dataSource.toJson();
		object.add("dataSource", jsonDataSource);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("sales")) {
			JsonElement jsonSales = jsonObject.get("sales");
			if (jsonSales != null) {
				sales = new ArrayList<Sale>();
				Sale item = null;
				for (int i = 0; i < jsonSales.getAsJsonArray().size(); i++) {
					if (jsonSales.getAsJsonArray().get(i) != null) {
						(item = new Sale()).fromJson(jsonSales.getAsJsonArray().get(i).getAsJsonObject());
						sales.add(item);
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
		if (jsonObject.has("items")) {
			JsonElement jsonItems = jsonObject.get("items");
			if (jsonItems != null) {
				items = new ArrayList<Item>();
				Item item = null;
				for (int i = 0; i < jsonItems.getAsJsonArray().size(); i++) {
					if (jsonItems.getAsJsonArray().get(i) != null) {
						(item = new Item()).fromJson(jsonItems.getAsJsonArray().get(i).getAsJsonObject());
						items.add(item);
					}
				}
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