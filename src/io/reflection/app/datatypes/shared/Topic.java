//  
//  Topic.java
//  reflection.io
//
//  Created by William Shakour on April 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Topic extends DataType {
	public User author;
	public List<String> tags;
	public List<Reply> replies;
	public Forum forum;
	public User lastReplier;
	public String title;
	public String content;
	public Integer heat;
	public Boolean sticky;
	public Integer flagged;
	public Boolean locked;
	public Date lastReplied;
	public Integer numberOfReplies;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonAuthor = author == null ? JsonNull.INSTANCE : author.toJson();
		object.add("author", jsonAuthor);
		JsonElement jsonTags = JsonNull.INSTANCE;
		if (tags != null) {
			jsonTags = new JsonArray();
			for (int i = 0; i < tags.size(); i++) {
				JsonElement jsonTagsItem = tags.get(i) == null ? JsonNull.INSTANCE : new JsonPrimitive(tags.get(i));
				((JsonArray) jsonTags).add(jsonTagsItem);
			}
		}
		object.add("tags", jsonTags);
		JsonElement jsonReplies = JsonNull.INSTANCE;
		if (replies != null) {
			jsonReplies = new JsonArray();
			for (int i = 0; i < replies.size(); i++) {
				JsonElement jsonRepliesItem = replies.get(i) == null ? JsonNull.INSTANCE : replies.get(i).toJson();
				((JsonArray) jsonReplies).add(jsonRepliesItem);
			}
		}
		object.add("replies", jsonReplies);
		JsonElement jsonForum = forum == null ? JsonNull.INSTANCE : forum.toJson();
		object.add("forum", jsonForum);
		JsonElement jsonLastReplier = lastReplier == null ? JsonNull.INSTANCE : lastReplier.toJson();
		object.add("lastReplier", jsonLastReplier);
		JsonElement jsonTitle = title == null ? JsonNull.INSTANCE : new JsonPrimitive(title);
		object.add("title", jsonTitle);
		JsonElement jsonContent = content == null ? JsonNull.INSTANCE : new JsonPrimitive(content);
		object.add("content", jsonContent);
		JsonElement jsonHeat = heat == null ? JsonNull.INSTANCE : new JsonPrimitive(heat);
		object.add("heat", jsonHeat);
		JsonElement jsonSticky = sticky == null ? JsonNull.INSTANCE : new JsonPrimitive(sticky);
		object.add("sticky", jsonSticky);
		JsonElement jsonFlagged = flagged == null ? JsonNull.INSTANCE : new JsonPrimitive(flagged);
		object.add("flagged", jsonFlagged);
		JsonElement jsonLocked = locked == null ? JsonNull.INSTANCE : new JsonPrimitive(locked);
		object.add("locked", jsonLocked);
		JsonElement jsonLastReplied = lastReplied == null ? JsonNull.INSTANCE : new JsonPrimitive(lastReplied.getTime());
		object.add("lastReplied", jsonLastReplied);
		JsonElement jsonNumberOfReplies = numberOfReplies == null ? JsonNull.INSTANCE : new JsonPrimitive(numberOfReplies);
		object.add("numberOfReplies", jsonNumberOfReplies);
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
		if (jsonObject.has("tags")) {
			JsonElement jsonTags = jsonObject.get("tags");
			if (jsonTags != null) {
				tags = new ArrayList<String>();
				String item = null;
				for (int i = 0; i < jsonTags.getAsJsonArray().size(); i++) {
					if (jsonTags.getAsJsonArray().get(i) != null) {
						item = jsonTags.getAsJsonArray().get(i).getAsString();
						tags.add(item);
					}
				}
			}
		}

		if (jsonObject.has("replies")) {
			JsonElement jsonReplies = jsonObject.get("replies");
			if (jsonReplies != null) {
				replies = new ArrayList<Reply>();
				Reply item = null;
				for (int i = 0; i < jsonReplies.getAsJsonArray().size(); i++) {
					if (jsonReplies.getAsJsonArray().get(i) != null) {
						(item = new Reply()).fromJson(jsonReplies.getAsJsonArray().get(i).getAsJsonObject());
						replies.add(item);
					}
				}
			}
		}

		if (jsonObject.has("forum")) {
			JsonElement jsonForum = jsonObject.get("forum");
			if (jsonForum != null) {
				forum = new Forum();
				forum.fromJson(jsonForum.getAsJsonObject());
			}
		}
		if (jsonObject.has("lastReplier")) {
			JsonElement jsonLastReplier = jsonObject.get("lastReplier");
			if (jsonLastReplier != null) {
				lastReplier = new User();
				lastReplier.fromJson(jsonLastReplier.getAsJsonObject());
			}
		}
		if (jsonObject.has("title")) {
			JsonElement jsonTitle = jsonObject.get("title");
			if (jsonTitle != null) {
				title = jsonTitle.getAsString();
			}
		}
		if (jsonObject.has("content")) {
			JsonElement jsonContent = jsonObject.get("content");
			if (jsonContent != null) {
				content = jsonContent.getAsString();
			}
		}
		if (jsonObject.has("heat")) {
			JsonElement jsonHeat = jsonObject.get("heat");
			if (jsonHeat != null) {
				heat = Integer.valueOf(jsonHeat.getAsInt());
			}
		}
		if (jsonObject.has("sticky")) {
			JsonElement jsonSticky = jsonObject.get("sticky");
			if (jsonSticky != null) {
				sticky = Boolean.valueOf(jsonSticky.getAsBoolean());
			}
		}
		if (jsonObject.has("flagged")) {
			JsonElement jsonFlagged = jsonObject.get("flagged");
			if (jsonFlagged != null) {
				flagged = Integer.valueOf(jsonFlagged.getAsInt());
			}
		}
		if (jsonObject.has("locked")) {
			JsonElement jsonLocked = jsonObject.get("locked");
			if (jsonLocked != null) {
				locked = Boolean.valueOf(jsonLocked.getAsBoolean());
			}
		}
		if (jsonObject.has("lastReplied")) {
			JsonElement jsonLastReplied = jsonObject.get("lastReplied");
			if (jsonLastReplied != null) {
				lastReplied = new Date(jsonLastReplied.getAsLong());
			}
		}
		if (jsonObject.has("numberOfReplies")) {
			JsonElement jsonNumberOfReplies = jsonObject.get("numberOfReplies");
			if (jsonNumberOfReplies != null) {
				numberOfReplies = Integer.valueOf(jsonNumberOfReplies.getAsInt());
			}
		}
	}
}