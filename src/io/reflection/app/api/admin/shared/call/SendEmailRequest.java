//  
//  SendEmailRequest.java
//  reflection.io
//
//  Created by William Shakour on February 20, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Request;
import io.reflection.app.datatypes.shared.EmailTemplate;
import io.reflection.app.datatypes.shared.User;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SendEmailRequest extends Request {
	public EmailTemplate template;
	public User toUser;
	public String toAddress;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonTemplate = template == null ? JsonNull.INSTANCE : template.toJson();
		object.add("template", jsonTemplate);
		JsonElement jsonToUser = toUser == null ? JsonNull.INSTANCE : toUser.toJson();
		object.add("toUser", jsonToUser);
		JsonElement jsonToAddress = toAddress == null ? JsonNull.INSTANCE : new JsonPrimitive(toAddress);
		object.add("toAddress", jsonToAddress);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("template")) {
			JsonElement jsonTemplate = jsonObject.get("template");
			if (jsonTemplate != null) {
				template = new EmailTemplate();
				template.fromJson(jsonTemplate.getAsJsonObject());
			}
		}
		if (jsonObject.has("toUser")) {
			JsonElement jsonToUser = jsonObject.get("toUser");
			if (jsonToUser != null) {
				toUser = new User();
				toUser.fromJson(jsonToUser.getAsJsonObject());
			}
		}
		if (jsonObject.has("toAddress")) {
			JsonElement jsonToAddress = jsonObject.get("toAddress");
			if (jsonToAddress != null) {
				toAddress = jsonToAddress.getAsString();
			}
		}
	}
}