//  
//  SimpleModelRun.java
//  reflection.io
//
//  Created by William Shakour on September 4, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SimpleModelRun extends DataType {
	public FeedFetch feedFetch;
	public Double a;
	public Double b;
	public Date summaryDate;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonFeedFetch = feedFetch == null ? JsonNull.INSTANCE : feedFetch.toJson();
		object.add("feedFetch", jsonFeedFetch);
		JsonElement jsonA = a == null ? JsonNull.INSTANCE : new JsonPrimitive(a);
		object.add("a", jsonA);
		JsonElement jsonB = b == null ? JsonNull.INSTANCE : new JsonPrimitive(b);
		object.add("b", jsonB);
		JsonElement jsonSummaryDate = summaryDate == null ? JsonNull.INSTANCE : new JsonPrimitive(summaryDate.getTime());
		object.add("summaryDate", jsonSummaryDate);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("feedFetch")) {
			JsonElement jsonFeedFetch = jsonObject.get("feedFetch");
			if (jsonFeedFetch != null) {
				feedFetch = new FeedFetch();
				feedFetch.fromJson(jsonFeedFetch.getAsJsonObject());
			}
		}
		if (jsonObject.has("a")) {
			JsonElement jsonA = jsonObject.get("a");
			if (jsonA != null) {
				a = Double.valueOf(jsonA.getAsDouble());
			}
		}
		if (jsonObject.has("b")) {
			JsonElement jsonB = jsonObject.get("b");
			if (jsonB != null) {
				b = Double.valueOf(jsonB.getAsDouble());
			}
		}
		if (jsonObject.has("summaryDate")) {
			JsonElement jsonSummaryDate = jsonObject.get("summaryDate");
			if (jsonSummaryDate != null) {
				summaryDate = new Date(jsonSummaryDate.getAsLong());
			}
		}
	}

	public SimpleModelRun feedFetch(FeedFetch feedFetch) {
		this.feedFetch = feedFetch;
		return this;
	}

	public SimpleModelRun a(Double a) {
		this.a = a;
		return this;
	}

	public SimpleModelRun b(Double b) {
		this.b = b;
		return this;
	}

	public SimpleModelRun summaryDate(Date summaryDate) {
		this.summaryDate = summaryDate;
		return this;
	}
}