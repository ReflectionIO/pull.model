//  
//  DataType.java
//  storedata
//
//  Created by William Shakour on 21 Jun 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.willshex.gson.json.shared.Jsonable;

/**
 * @author billy1380
 * 
 */
public abstract class DataType extends Jsonable {

	/**
	 * DataType Id
	 */
	public Long id = null;

	/**
	 * created date
	 */
	public Date created = null;

	// @Index(IfNotDefault.class) public String deleted = "n";

	public String deleted;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonId = id == null ? JsonNull.INSTANCE : new JsonPrimitive(id);
		object.add("id", jsonId);
		JsonElement jsonCreated = created == null ? JsonNull.INSTANCE : new JsonPrimitive(Long.valueOf(created.getTime()));
		object.add("created", jsonCreated);
		JsonElement jsonDeleted = deleted == null ? JsonNull.INSTANCE : new JsonPrimitive(deleted);
		object.add("deleted", jsonDeleted);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("id")) {
			JsonElement jsonId = jsonObject.get("id");
			if (jsonId != null) {
				id = Long.valueOf(jsonId.getAsLong());
			}
		}
		if (jsonObject.has("created")) {
			JsonElement jsonCreated = jsonObject.get("created");
			if (jsonCreated != null) {
				created = new Date(jsonCreated.getAsLong());
			}
		}
		if (jsonObject.has("deleted")) {
			JsonElement jsonDeleted = jsonObject.get("deleted");
			if (jsonDeleted != null) {
				deleted = jsonDeleted.getAsString();
			}
		}
	}
}
