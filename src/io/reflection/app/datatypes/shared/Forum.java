//  
//  Forum.java
//  reflection.io
//
//  Created by William Shakour on April 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Forum extends DataType {
	public List<User> members;
	public User creator;
	public List<Topic> topics;
	public String title;
	public String description;
	public ForumTypeType type;
	public Integer numberOfTopics;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonMembers = JsonNull.INSTANCE;
		if (members != null) {
			jsonMembers = new JsonArray();
			for (int i = 0; i < members.size(); i++) {
				JsonElement jsonMembersItem = members.get(i) == null ? JsonNull.INSTANCE : members.get(i).toJson();
				((JsonArray) jsonMembers).add(jsonMembersItem);
			}
		}
		object.add("members", jsonMembers);
		JsonElement jsonCreator = creator == null ? JsonNull.INSTANCE : creator.toJson();
		object.add("creator", jsonCreator);
		JsonElement jsonTopics = JsonNull.INSTANCE;
		if (topics != null) {
			jsonTopics = new JsonArray();
			for (int i = 0; i < topics.size(); i++) {
				JsonElement jsonTopicsItem = topics.get(i) == null ? JsonNull.INSTANCE : topics.get(i).toJson();
				((JsonArray) jsonTopics).add(jsonTopicsItem);
			}
		}
		object.add("topics", jsonTopics);
		JsonElement jsonTitle = title == null ? JsonNull.INSTANCE : new JsonPrimitive(title);
		object.add("title", jsonTitle);
		JsonElement jsonDescription = description == null ? JsonNull.INSTANCE : new JsonPrimitive(description);
		object.add("description", jsonDescription);
		JsonElement jsonType = type == null ? JsonNull.INSTANCE : new JsonPrimitive(type.toString());
		object.add("type", jsonType);
		JsonElement jsonNumberOfTopics = numberOfTopics == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfTopics);
		object.add("numberOfTopics", jsonNumberOfTopics);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("members")) {
			JsonElement jsonMembers = jsonObject.get("members");
			if (jsonMembers != null) {
				members = new ArrayList<User>();
				User item = null;
				for (int i = 0; i < jsonMembers.getAsJsonArray().size(); i++) {
					if (jsonMembers.getAsJsonArray().get(i) != null) {
						(item = new User()).fromJson(jsonMembers.getAsJsonArray().get(i).getAsJsonObject());
						members.add(item);
					}
				}
			}
		}

		if (jsonObject.has("creator")) {
			JsonElement jsonCreator = jsonObject.get("creator");
			if (jsonCreator != null) {
				creator = new User();
				creator.fromJson(jsonCreator.getAsJsonObject());
			}
		}
		if (jsonObject.has("topics")) {
			JsonElement jsonTopics = jsonObject.get("topics");
			if (jsonTopics != null) {
				topics = new ArrayList<Topic>();
				Topic item = null;
				for (int i = 0; i < jsonTopics.getAsJsonArray().size(); i++) {
					if (jsonTopics.getAsJsonArray().get(i) != null) {
						(item = new Topic()).fromJson(jsonTopics.getAsJsonArray().get(i).getAsJsonObject());
						topics.add(item);
					}
				}
			}
		}

		if (jsonObject.has("title")) {
			JsonElement jsonTitle = jsonObject.get("title");
			if (jsonTitle != null) {
				title = jsonTitle.getAsString();
			}
		}
		if (jsonObject.has("description")) {
			JsonElement jsonDescription = jsonObject.get("description");
			if (jsonDescription != null) {
				description = jsonDescription.getAsString();
			}
		}
		if (jsonObject.has("type")) {
			JsonElement jsonType = jsonObject.get("type");
			if (jsonType != null) {
				type = ForumTypeType.fromString(jsonType.getAsString());
			}
		}
		if (jsonObject.has("numberOfTopics")) {
			JsonElement jsonNumberOfTopics = jsonObject.get("numberOfTopics");
			if (jsonNumberOfTopics != null) {
				numberOfTopics = Integer.valueOf(jsonNumberOfTopics.getAsInt());
			}
		}
	}
}