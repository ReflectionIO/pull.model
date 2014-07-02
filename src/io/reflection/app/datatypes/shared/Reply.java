//  
//  Reply.java
//  reflection.io
//
//  Created by William Shakour on April 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Reply extends DataType {
	public User author;
	public Topic topic;
	public String content;
	public Integer flagged;
	public Boolean solution;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonAuthor = author == null ? JsonNull.INSTANCE : author.toJson();
		object.add("author", jsonAuthor);
		JsonElement jsonTopic = topic == null ? JsonNull.INSTANCE : topic.toJson();
		object.add("topic", jsonTopic);
		JsonElement jsonContent = content == null ? JsonNull.INSTANCE : new JsonPrimitive(content);
		object.add("content", jsonContent);
		JsonElement jsonFlagged = flagged == null ? JsonNull.INSTANCE : new JsonPrimitive(flagged);
		object.add("flagged", jsonFlagged);
		JsonElement jsonSolution = solution == null ? JsonNull.INSTANCE : new JsonPrimitive(solution);
		object.add("solution", jsonSolution);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("author")) {
			JsonElement jsonAuthor = jsonObject.get("author");
			if (jsonAuthor != null) {
				author = new User();
				author.fromJson(jsonAuthor.getAsJsonObject());
			}
		}
		if (jsonObject.has("topic")) {
			JsonElement jsonTopic = jsonObject.get("topic");
			if (jsonTopic != null) {
				topic = new Topic();
				topic.fromJson(jsonTopic.getAsJsonObject());
			}
		}
		if (jsonObject.has("content")) {
			JsonElement jsonContent = jsonObject.get("content");
			if (jsonContent != null) {
				content = jsonContent.getAsString();
			}
		}
		if (jsonObject.has("flagged")) {
			JsonElement jsonFlagged = jsonObject.get("flagged");
			if (jsonFlagged != null) {
				flagged = Integer.valueOf(jsonFlagged.getAsInt());
			}
		}
		if (jsonObject.has("solution")) {
			JsonElement jsonSolution = jsonObject.get("solution");
			if (jsonSolution != null) {
				solution = Boolean.valueOf(jsonSolution.getAsBoolean());
			}
		}
	}
}