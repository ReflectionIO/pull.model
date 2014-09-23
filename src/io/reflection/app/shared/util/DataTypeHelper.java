//
//  DataTypeHelper.java
//  storedata
//
//  Created by William Shakour (billy1380) on 20 May 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.app.shared.util;

import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.Permission;
import io.reflection.app.datatypes.shared.Rank;
import io.reflection.app.datatypes.shared.Role;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * @author billy1380
 * 
 */
public class DataTypeHelper {

	public static final String ACTIVE_VALUE = "y";
	public static final String INACTIVE_VALUE = "n";

	public static final long PERMISSION_FULL_RANK_VIEW_ID = 1;
	public static final long PERMISSION_HAS_LINKED_ACCOUNT_ID = 20;

	public static final long ROLE_ADMIN_ID = 1;
	public static final long ROLE_DEVELOPER_ID = 2;
	public static final long ROLE_PREMIUM_ID = 3;
	public static final long ROLE_ALPHA_ID = 4;
	public static final long ROLE_BETA_ID = 5;

	/**
	 * Creates a role with a given id
	 * 
	 * @param id
	 * @return
	 */
	public static Role createRole(Long id) {
		Role role = new Role();
		role.id = id;
		return role;
	}

	/**
	 * Creates a permission with a given id
	 * 
	 * @param id
	 * @return
	 */
	public static Permission createPermission(Long id) {
		Permission permission = new Permission();
		permission.id = id;
		return permission;
	}

	public static String itemIapState(Item item, String yes, String no, String unknown) {
		String usesIap = null;

		if (item != null) {
			usesIap = jsonPropertiesIapState(item.properties, yes, no, unknown);
		} else {
			usesIap = unknown;
		}

		return usesIap;
	}

	public static String jsonPropertiesIapState(String jsonProperties, String yes, String no, String unknown) {
		String usesIap = null;

		if (jsonProperties != null) {
			JsonElement propertiesJsonElement = (new JsonParser()).parse(jsonProperties);

			if (propertiesJsonElement.isJsonObject()) {
				JsonObject propertiesJsonObject = propertiesJsonElement.getAsJsonObject();
				JsonElement usesIapJsonElement = propertiesJsonObject.get("usesIap");

				if (usesIapJsonElement != null && usesIapJsonElement.isJsonPrimitive()) {
					if (usesIapJsonElement.getAsBoolean()) {
						usesIap = yes;
					} else {
						usesIap = no;
					}
				}
			}
		}

		if (usesIap == null) {
			usesIap = unknown;
		}

		return usesIap;
	}

	public static void sortRanksByDate(List<Rank> ranks) {
		if (ranks != null) {
			Collections.sort(ranks, new Comparator<Rank>() {

				@Override
				public int compare(Rank r1, Rank r2) {
					return r1.date.getTime() > r2.date.getTime() ? 1 : (r2.date.getTime() > r1.date.getTime() ? -1 : 0);
				}

			});
		}
	}

	public static void sortItemsByDate(List<Item> items) {
		if (items != null) {
			Collections.sort(items, new Comparator<Item>() {

				@Override
				public int compare(Item i1, Item i2) {
					return i1.added.getTime() > i2.added.getTime() ? 1 : (i2.added.getTime() > i1.added.getTime() ? -1 : 0);
				}

			});
		}
	}
}
