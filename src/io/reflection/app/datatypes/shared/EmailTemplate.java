//  
//  EmailTemplate.java
//  reflection.io
//
//  Created by William Shakour on February 17, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class EmailTemplate extends DataType {
	public String from;
	public String body;
	public String subject;
	public EmailFormatType format;
	public EmailTypeType type;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonFrom = from == null ? JsonNull.INSTANCE : new JsonPrimitive(from);
		object.add("from", jsonFrom);
		JsonElement jsonBody = body == null ? JsonNull.INSTANCE : new JsonPrimitive(body);
		object.add("body", jsonBody);
		JsonElement jsonSubject = subject == null ? JsonNull.INSTANCE : new JsonPrimitive(subject);
		object.add("subject", jsonSubject);
		JsonElement jsonFormat = format == null ? JsonNull.INSTANCE : new JsonPrimitive(format.toString());
		object.add("format", jsonFormat);
		JsonElement jsonType = type == null ? JsonNull.INSTANCE : new JsonPrimitive(type.toString());
		object.add("type", jsonType);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("from")) {
			JsonElement jsonFrom = jsonObject.get("from");
			if (jsonFrom != null) {
				from = jsonFrom.getAsString();
			}
		}
		if (jsonObject.has("body")) {
			JsonElement jsonBody = jsonObject.get("body");
			if (jsonBody != null) {
				body = jsonBody.getAsString();
			}
		}
		if (jsonObject.has("subject")) {
			JsonElement jsonSubject = jsonObject.get("subject");
			if (jsonSubject != null) {
				subject = jsonSubject.getAsString();
			}
		}
		if (jsonObject.has("format")) {
			JsonElement jsonFormat = jsonObject.get("format");
			if (jsonFormat != null) {
				format = EmailFormatType.fromString(jsonFormat.getAsString());
			}
		}
		if (jsonObject.has("type")) {
			JsonElement jsonType = jsonObject.get("type");
			if (jsonType != null) {
				type = EmailTypeType.fromString(jsonType.getAsString());
			}
		}
	}
}