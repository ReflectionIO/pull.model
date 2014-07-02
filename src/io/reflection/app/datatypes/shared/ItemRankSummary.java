package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ItemRankSummary extends DataType {

	public String itemId;

	public String type;

	public String source;

	public Integer numberOfTimesRanked = Integer.valueOf(0);

	public Integer numberOfTimesRankedTop10 = Integer.valueOf(0);

	public Integer numberOfTimesRankedTop25 = Integer.valueOf(0);

	public Integer numberOfTimesRankedTop50 = Integer.valueOf(0);

	public Integer numberOfTimesRankedTop100 = Integer.valueOf(0);

	public Integer numberOfTimesRankedTop200 = Integer.valueOf(0);

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonItemId = itemId == null ? JsonNull.INSTANCE : new JsonPrimitive(itemId);
		object.add("itemId", jsonItemId);
		JsonElement jsonType = type == null ? JsonNull.INSTANCE : new JsonPrimitive(type);
		object.add("type", jsonType);
		JsonElement jsonSource = source == null ? JsonNull.INSTANCE : new JsonPrimitive(source);
		object.add("source", jsonSource);
		JsonElement jsonNumberOfTimesRanked = numberOfTimesRanked == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRanked);
		object.add("numberOfTimesRanked", jsonNumberOfTimesRanked);
		JsonElement jsonNumberOfTimesRankedTop10 = numberOfTimesRankedTop10 == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRankedTop10);
		object.add("numberOfTimesRankedTop10", jsonNumberOfTimesRankedTop10);
		JsonElement jsonNumberOfTimesRankedTop25 = numberOfTimesRankedTop25 == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRankedTop25);
		object.add("numberOfTimesRankedTop25", jsonNumberOfTimesRankedTop25);
		JsonElement jsonNumberOfTimesRankedTop50 = numberOfTimesRankedTop50 == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRankedTop50);
		object.add("numberOfTimesRankedTop50", jsonNumberOfTimesRankedTop50);
		JsonElement jsonNumberOfTimesRankedTop100 = numberOfTimesRankedTop100 == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRankedTop100);
		object.add("numberOfTimesRankedTop100", jsonNumberOfTimesRankedTop100);
		JsonElement jsonNumberOfTimesRankedTop200 = numberOfTimesRankedTop200 == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTimesRankedTop200);
		object.add("numberOfTimesRankedTop200", jsonNumberOfTimesRankedTop200);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
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
		if (jsonObject.has("source")) {
			JsonElement jsonSource = jsonObject.get("source");
			if (jsonSource != null) {
				source = jsonSource.getAsString();
			}
		}
		if (jsonObject.has("numberOfTimesRanked")) {
			JsonElement jsonNumberOfTimesRanked = jsonObject.get("numberOfTimesRanked");
			if (jsonNumberOfTimesRanked != null) {
				numberOfTimesRanked = Integer.valueOf(jsonNumberOfTimesRanked.getAsInt());
			}
		}
		if (jsonObject.has("numberOfTimesRankedTop10")) {
			JsonElement jsonNumberOfTimesRankedTop10 = jsonObject.get("numberOfTimesRankedTop10");
			if (jsonNumberOfTimesRankedTop10 != null) {
				numberOfTimesRankedTop10 = Integer.valueOf(jsonNumberOfTimesRankedTop10.getAsInt());
			}
		}
		if (jsonObject.has("numberOfTimesRankedTop25")) {
			JsonElement jsonNumberOfTimesRankedTop25 = jsonObject.get("numberOfTimesRankedTop25");
			if (jsonNumberOfTimesRankedTop25 != null) {
				numberOfTimesRankedTop25 = Integer.valueOf(jsonNumberOfTimesRankedTop25.getAsInt());
			}
		}
		if (jsonObject.has("numberOfTimesRankedTop50")) {
			JsonElement jsonNumberOfTimesRankedTop50 = jsonObject.get("numberOfTimesRankedTop50");
			if (jsonNumberOfTimesRankedTop50 != null) {
				numberOfTimesRankedTop50 = Integer.valueOf(jsonNumberOfTimesRankedTop50.getAsInt());
			}
		}
		if (jsonObject.has("numberOfTimesRankedTop100")) {
			JsonElement jsonNumberOfTimesRankedTop100 = jsonObject.get("numberOfTimesRankedTop100");
			if (jsonNumberOfTimesRankedTop100 != null) {
				numberOfTimesRankedTop100 = Integer.valueOf(jsonNumberOfTimesRankedTop100.getAsInt());
			}
		}
		if (jsonObject.has("numberOfTimesRankedTop200")) {
			JsonElement jsonNumberOfTimesRankedTop200 = jsonObject.get("numberOfTimesRankedTop200");
			if (jsonNumberOfTimesRankedTop200 != null) {
				numberOfTimesRankedTop200 = Integer.valueOf(jsonNumberOfTimesRankedTop200.getAsInt());
			}
		}
	}
}
