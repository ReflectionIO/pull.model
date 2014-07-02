//  
//  Post.java
//  reflection.io
//
//  Created by William Shakour on March 16, 2014.
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

public class Post extends DataType {
	public User author;
	public List<String> tags;
	public Date published;
	public String title;
	public String description;
	public String content;
	public Boolean visible;
	public Boolean commentsEnabled;

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
		JsonElement jsonPublished = published == null ? JsonNull.INSTANCE : new JsonPrimitive(published.getTime());
		object.add("published", jsonPublished);
		JsonElement jsonTitle = title == null ? JsonNull.INSTANCE : new JsonPrimitive(title);
		object.add("title", jsonTitle);
		JsonElement jsonDescription = description == null ? JsonNull.INSTANCE : new JsonPrimitive(description);
		object.add("description", jsonDescription);
		JsonElement jsonContent = content == null ? JsonNull.INSTANCE : new JsonPrimitive(content);
		object.add("content", jsonContent);
		JsonElement jsonVisible = visible == null ? JsonNull.INSTANCE : new JsonPrimitive(visible);
		object.add("visible", jsonVisible);
		JsonElement jsonCommentsEnabled = commentsEnabled == null ? JsonNull.INSTANCE : new JsonPrimitive(commentsEnabled);
		object.add("commentsEnabled", jsonCommentsEnabled);
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

		if (jsonObject.has("published")) {
			JsonElement jsonPublished = jsonObject.get("published");
			if (jsonPublished != null) {
				published = new Date(jsonPublished.getAsLong());
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
		if (jsonObject.has("content")) {
			JsonElement jsonContent = jsonObject.get("content");
			if (jsonContent != null) {
				content = jsonContent.getAsString();
			}
		}
		if (jsonObject.has("visible")) {
			JsonElement jsonVisible = jsonObject.get("visible");
			if (jsonVisible != null) {
				visible = Boolean.valueOf(jsonVisible.getAsBoolean());
			}
		}
		if (jsonObject.has("commentsEnabled")) {
			JsonElement jsonCommentsEnabled = jsonObject.get("commentsEnabled");
			if (jsonCommentsEnabled != null) {
				commentsEnabled = Boolean.valueOf(jsonCommentsEnabled.getAsBoolean());
			}
		}
	}
}