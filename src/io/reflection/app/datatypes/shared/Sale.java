//  
//  Sale.java
//  reflection.io
//
//  Created by William Shakour on December 30, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Sale extends DataType {
	public DataAccount account;
	public Item item;
	public String country;
	public String sku;
	public String developer;
	public String title;
	public String version;
	public String typeIdentifier;
	public Integer units;
	public Float proceeds;
	public String currency;
	public Date begin;
	public Date end;
	public String customerCurrency;
	public Float customerPrice;
	public String promoCode;
	public String parentIdentifier;
	public String subscription;
	public String period;
	public String category;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonAccount = account == null ? JsonNull.INSTANCE : account.toJson();
		object.add("account", jsonAccount);
		JsonElement jsonItem = item == null ? JsonNull.INSTANCE : item.toJson();
		object.add("item", jsonItem);
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : new JsonPrimitive(country);
		object.add("country", jsonCountry);
		JsonElement jsonSku = sku == null ? JsonNull.INSTANCE : new JsonPrimitive(sku);
		object.add("sku", jsonSku);
		JsonElement jsonDeveloper = developer == null ? JsonNull.INSTANCE : new JsonPrimitive(developer);
		object.add("developer", jsonDeveloper);
		JsonElement jsonTitle = title == null ? JsonNull.INSTANCE : new JsonPrimitive(title);
		object.add("title", jsonTitle);
		JsonElement jsonVersion = version == null ? JsonNull.INSTANCE : new JsonPrimitive(version);
		object.add("version", jsonVersion);
		JsonElement jsonTypeIdentifier = typeIdentifier == null ? JsonNull.INSTANCE : new JsonPrimitive(typeIdentifier);
		object.add("typeIdentifier", jsonTypeIdentifier);
		JsonElement jsonUnits = units == null ? JsonNull.INSTANCE : new JsonPrimitive(units);
		object.add("units", jsonUnits);
		JsonElement jsonProceeds = proceeds == null ? JsonNull.INSTANCE : new JsonPrimitive(proceeds);
		object.add("proceeds", jsonProceeds);
		JsonElement jsonCurrency = currency == null ? JsonNull.INSTANCE : new JsonPrimitive(currency);
		object.add("currency", jsonCurrency);
		JsonElement jsonBegin = begin == null ? JsonNull.INSTANCE : new JsonPrimitive(begin.getTime());
		object.add("begin", jsonBegin);
		JsonElement jsonEnd = end == null ? JsonNull.INSTANCE : new JsonPrimitive(end.getTime());
		object.add("end", jsonEnd);
		JsonElement jsonCustomerCurrency = customerCurrency == null ? JsonNull.INSTANCE : new JsonPrimitive(customerCurrency);
		object.add("customerCurrency", jsonCustomerCurrency);
		JsonElement jsonCustomerPrice = customerPrice == null ? JsonNull.INSTANCE : new JsonPrimitive(customerPrice);
		object.add("customerPrice", jsonCustomerPrice);
		JsonElement jsonPromoCode = promoCode == null ? JsonNull.INSTANCE : new JsonPrimitive(promoCode);
		object.add("promoCode", jsonPromoCode);
		JsonElement jsonParentIdentifier = parentIdentifier == null ? JsonNull.INSTANCE : new JsonPrimitive(parentIdentifier);
		object.add("parentIdentifier", jsonParentIdentifier);
		JsonElement jsonSubscription = subscription == null ? JsonNull.INSTANCE : new JsonPrimitive(subscription);
		object.add("subscription", jsonSubscription);
		JsonElement jsonPeriod = period == null ? JsonNull.INSTANCE : new JsonPrimitive(period);
		object.add("period", jsonPeriod);
		JsonElement jsonCategory = category == null ? JsonNull.INSTANCE : new JsonPrimitive(category);
		object.add("category", jsonCategory);
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
		if (jsonObject.has("item")) {
			JsonElement jsonItem = jsonObject.get("item");
			if (jsonItem != null) {
				item = new Item();
				item.fromJson(jsonItem.getAsJsonObject());
			}
		}
		if (jsonObject.has("country")) {
			JsonElement jsonCountry = jsonObject.get("country");
			if (jsonCountry != null) {
				country = jsonCountry.getAsString();
			}
		}
		if (jsonObject.has("sku")) {
			JsonElement jsonSku = jsonObject.get("sku");
			if (jsonSku != null) {
				sku = jsonSku.getAsString();
			}
		}
		if (jsonObject.has("developer")) {
			JsonElement jsonDeveloper = jsonObject.get("developer");
			if (jsonDeveloper != null) {
				developer = jsonDeveloper.getAsString();
			}
		}
		if (jsonObject.has("title")) {
			JsonElement jsonTitle = jsonObject.get("title");
			if (jsonTitle != null) {
				title = jsonTitle.getAsString();
			}
		}
		if (jsonObject.has("version")) {
			JsonElement jsonVersion = jsonObject.get("version");
			if (jsonVersion != null) {
				version = jsonVersion.getAsString();
			}
		}
		if (jsonObject.has("typeIdentifier")) {
			JsonElement jsonTypeIdentifier = jsonObject.get("typeIdentifier");
			if (jsonTypeIdentifier != null) {
				typeIdentifier = jsonTypeIdentifier.getAsString();
			}
		}
		if (jsonObject.has("units")) {
			JsonElement jsonUnits = jsonObject.get("units");
			if (jsonUnits != null) {
				units = Integer.valueOf(jsonUnits.getAsInt());
			}
		}
		if (jsonObject.has("proceeds")) {
			JsonElement jsonProceeds = jsonObject.get("proceeds");
			if (jsonProceeds != null) {
				proceeds = Float.valueOf(jsonProceeds.getAsFloat());
			}
		}
		if (jsonObject.has("currency")) {
			JsonElement jsonCurrency = jsonObject.get("currency");
			if (jsonCurrency != null) {
				currency = jsonCurrency.getAsString();
			}
		}
		if (jsonObject.has("begin")) {
			JsonElement jsonBegin = jsonObject.get("begin");
			if (jsonBegin != null) {
				begin = new Date(jsonBegin.getAsLong());
			}
		}
		if (jsonObject.has("end")) {
			JsonElement jsonEnd = jsonObject.get("end");
			if (jsonEnd != null) {
				end = new Date(jsonEnd.getAsLong());
			}
		}
		if (jsonObject.has("customerCurrency")) {
			JsonElement jsonCustomerCurrency = jsonObject.get("customerCurrency");
			if (jsonCustomerCurrency != null) {
				customerCurrency = jsonCustomerCurrency.getAsString();
			}
		}
		if (jsonObject.has("customerPrice")) {
			JsonElement jsonCustomerPrice = jsonObject.get("customerPrice");
			if (jsonCustomerPrice != null) {
				customerPrice = Float.valueOf(jsonCustomerPrice.getAsFloat());
			}
		}
		if (jsonObject.has("promoCode")) {
			JsonElement jsonPromoCode = jsonObject.get("promoCode");
			if (jsonPromoCode != null) {
				promoCode = jsonPromoCode.getAsString();
			}
		}
		if (jsonObject.has("parentIdentifier")) {
			JsonElement jsonParentIdentifier = jsonObject.get("parentIdentifier");
			if (jsonParentIdentifier != null) {
				parentIdentifier = jsonParentIdentifier.getAsString();
			}
		}
		if (jsonObject.has("subscription")) {
			JsonElement jsonSubscription = jsonObject.get("subscription");
			if (jsonSubscription != null) {
				subscription = jsonSubscription.getAsString();
			}
		}
		if (jsonObject.has("period")) {
			JsonElement jsonPeriod = jsonObject.get("period");
			if (jsonPeriod != null) {
				period = jsonPeriod.getAsString();
			}
		}
		if (jsonObject.has("category")) {
			JsonElement jsonCategory = jsonObject.get("category");
			if (jsonCategory != null) {
				category = jsonCategory.getAsString();
			}
		}
	}
}