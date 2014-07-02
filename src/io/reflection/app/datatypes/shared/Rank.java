/**
 * 
 */
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * @author billy1380
 * 
 */
public class Rank extends DataType {

	public Integer position = Integer.valueOf(0);

	public Integer grossingPosition = Integer.valueOf(0);

	public String itemId;

	public String type;

	public String country;

	public Date date;

	public String source;

	public Float price = Float.valueOf(0);

	public String currency;

	public Long code;

	public Category category;

	public Float revenue;
	public Integer downloads;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonCategory = category == null ? JsonNull.INSTANCE : category.toJson();
		object.add("category", jsonCategory);
		JsonElement jsonPosition = position == null ? JsonNull.INSTANCE : new JsonPrimitive(position);
		object.add("position", jsonPosition);
		JsonElement jsonGrossingPosition = grossingPosition == null ? JsonNull.INSTANCE : new JsonPrimitive(grossingPosition);
		object.add("grossingPosition", jsonGrossingPosition);
		JsonElement jsonItemId = itemId == null ? JsonNull.INSTANCE : new JsonPrimitive(itemId);
		object.add("itemId", jsonItemId);
		JsonElement jsonType = type == null ? JsonNull.INSTANCE : new JsonPrimitive(type);
		object.add("type", jsonType);
		JsonElement jsonCountry = country == null ? JsonNull.INSTANCE : new JsonPrimitive(country);
		object.add("country", jsonCountry);
		JsonElement jsonDate = date == null ? JsonNull.INSTANCE : new JsonPrimitive(date.getTime());
		object.add("date", jsonDate);
		JsonElement jsonSource = source == null ? JsonNull.INSTANCE : new JsonPrimitive(source);
		object.add("source", jsonSource);
		JsonElement jsonPrice = price == null ? JsonNull.INSTANCE : new JsonPrimitive(price);
		object.add("price", jsonPrice);
		JsonElement jsonCurrency = currency == null ? JsonNull.INSTANCE : new JsonPrimitive(currency);
		object.add("currency", jsonCurrency);
		JsonElement jsonCode = code == null ? JsonNull.INSTANCE : new JsonPrimitive(code);
		object.add("code", jsonCode);
		JsonElement jsonRevenue = revenue == null ? JsonNull.INSTANCE : new JsonPrimitive(revenue);
		object.add("revenue", jsonRevenue);
		JsonElement jsonDownloads = downloads == null ? JsonNull.INSTANCE : new JsonPrimitive(downloads);
		object.add("downloads", jsonDownloads);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("category")) {
			JsonElement jsonCategory = jsonObject.get("category");
			if (jsonCategory != null) {
				category = new Category();
				category.fromJson(jsonCategory.getAsJsonObject());
			}
		}
		if (jsonObject.has("position")) {
			JsonElement jsonPosition = jsonObject.get("position");
			if (jsonPosition != null) {
				position = Integer.valueOf(jsonPosition.getAsInt());
			}
		}
		if (jsonObject.has("grossingPosition")) {
			JsonElement jsonGrossingPosition = jsonObject.get("grossingPosition");
			if (jsonGrossingPosition != null) {
				grossingPosition = Integer.valueOf(jsonGrossingPosition.getAsInt());
			}
		}
		if (jsonObject.has("itemId")) {
			JsonElement jsonItemId = jsonObject.get("itemId");
			if (jsonItemId != null) {
				itemId = jsonItemId.getAsString();
			}
		}
		if (jsonObject.has("type")) {
			JsonElement jsonType = jsonObject.get("type");
			if (jsonType != null) {
				type = jsonType.getAsString();
			}
		}
		if (jsonObject.has("country")) {
			JsonElement jsonCountry = jsonObject.get("country");
			if (jsonCountry != null) {
				country = jsonCountry.getAsString();
			}
		}
		if (jsonObject.has("date")) {
			JsonElement jsonDate = jsonObject.get("date");
			if (jsonDate != null) {
				date = new Date(jsonDate.getAsLong());
			}
		}
		if (jsonObject.has("source")) {
			JsonElement jsonSource = jsonObject.get("source");
			if (jsonSource != null) {
				source = jsonSource.getAsString();
			}
		}
		if (jsonObject.has("price")) {
			JsonElement jsonPrice = jsonObject.get("price");
			if (jsonPrice != null) {
				price = Float.valueOf(jsonPrice.getAsFloat());
			}
		}
		if (jsonObject.has("currency")) {
			JsonElement jsonCurrency = jsonObject.get("currency");
			if (jsonCurrency != null) {
				currency = jsonCurrency.getAsString();
			}
		}
		if (jsonObject.has("code")) {
			JsonElement jsonCode = jsonObject.get("code");
			if (jsonCode != null) {
				code = Long.valueOf(jsonCode.getAsLong());
			}
		}
		if (jsonObject.has("revenue")) {
			JsonElement jsonRevenue = jsonObject.get("revenue");
			if (jsonRevenue != null) {
				revenue = Float.valueOf(jsonRevenue.getAsFloat());
			}
		}
		if (jsonObject.has("downloads")) {
			JsonElement jsonDownloads = jsonObject.get("downloads");
			if (jsonDownloads != null) {
				downloads = Integer.valueOf(jsonDownloads.getAsInt());
			}
		}
	}
}