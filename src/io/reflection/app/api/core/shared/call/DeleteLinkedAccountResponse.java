//
//  DeleteLinkedAccountResponse.java
//  storedata
//
//  Created by William Shakour (stefanocapuzzi) on 14 Apr 2014.
//  Copyright �� 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.api.core.shared.call;

import io.reflection.app.api.shared.datatypes.Response;

import com.google.gson.JsonObject;

/**
 * @author stefanocapuzzi
 * 
 */
public class DeleteLinkedAccountResponse extends Response {
	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
	}
}
