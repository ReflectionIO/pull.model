//
//  ValidationError.java
//  storedata
//
//  Created by William Shakour on 5 Jul 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.api.shared;

/**
 * @author billy1380
 * 
 */
public enum ApiError {

	JsonParseException(100000, "Invalid Json, could not be parsed"),

	AccessCodeNull(100001, "Invalid value null for String: {0}.accessCode"),

	TokenNoMatch(100002, "Invalid value does not match scheme for String: {0}"),

	StoreNull(100003, "Invalid value null for Store: {0}.store"),
	StoreNotFound(100004, "Store not found Store: {0}.value"),
	StoreNoLookup(100005, "Invalid store lookup, need either id or a3Code or name for Store: {0}.store"),

	SearchQueryNull(100006, "Invalid value null search query for String: {0}.query"),

	PagerStartLargerThanTotal(100007, "Invalid value for start, should be less than totalCount for Pager: {0}.pager"),
	PagerStartNegative(100008, "Invalid negative value for Long: {0}.pager.start"),
	PagerCountTooSmall(100009, "Invalid 0 or negative value for Long: {0}.pager.count"),
	PagerCountTooLarge(100010, "Invalid value, maximum count should be <= 30 Long: {0}.pager.count"),

	CountryNull(100011, "Invalid value null for Country: {0}.country"),
	CountryNotFound(100012, "Country not found Country: {0}.country"),
	CountryNoLookup(100013, "Invalid country lookup, need either id or a2Code or name for Country: {0}.country"),

	InvalidValueNull(100014, "Invalid value null {0}"),

	ItemNull(100015, "Invalid value null for Item: {0}.item"),
	ItemNotFound(100016, "Item not found Item: {0}.item"),
	ItemNoLookup(100017, "Invalid item lookup, need either id or externalId or internalId for Item: {0}.item"),

	DateRangeOutOfBounds(100018, "Invalid date range, should be between {0}"),

	UserNull(100019, "Invalid value null for User: {0}.user"),

	StringNull(100020, "Invalid value null or empty for String: {0}"),
	BadEmailFormat(100021, "Invalid email address for Email: {0}"),
	ListTypeNotFound(100022, "List type not found for String: {0}"),

	UserNotFound(100023, "User not found User: {0}"),
	UserNoLookup(100024, "Invalid user lookup, need either id or username for User: {0}"),

	InvalidStringTooShort(100025, "Invalid value too short ({0}-{1}): {2}"),
	InvalidStringTooLong(100026, "Invalid value too long ({0}-{1}): {2}"),

	RoleNull(100027, "Invalid value null for Role: {0}"),
	RoleNotFound(100028, "Role not found Role: {0}"),
	RoleNoLookup(100029, "Invalid user role lookup, need either id or name for Role: {0}"),

	PermissionNull(100030, "Invalid value null for Permission: {0}"),
	PermissionNotFound(100031, "Permission not found Permission: {0}"),
	PermissionNoLookup(100032, "Invalid permission lookup, need either id or name for Permission: {0}"),

	SessionNull(100033, "Invalid value null for Session: {0}"),
	SessionNotFound(100034, "Session not found Session: {0}"),
	SessionNoLookup(100035, "Invalid session lookup, need either id or token for Session: {0}"),

	IncorrectPasswordForChange(100037, "Incorrect current password for user: {0}"),
	InvalidPasswordSameAsCurrent(100038, "Invalid password, current and new passwords are identical: {0}"),

	DataSourceNull(100039, "Invalid value null for DataSource: {0}"),
	DataSourceNotFound(100040, "Data source not found DataSource: {0}"),
	DataSourceNoLookup(100041, "Invalid data source lookup, need either id, a3Code or name for DataSource: {0}"),

	DataAccountNull(100042, "Invalid value null for DataAccount: {0}"),
	DataAccountNotFound(100043, "Data account not found DataAccount: {0}"),
	DataAccountNoLookup(100044, "Invalid data account lookup, no id provided for DataAccount: {0}"),

	CategoryNull(100045, "Invalid value null for Category: {0}"),
	CategoryNotFound(100046, "Category not found Category: {0}"),
	CategoryNoLookup(100047, "Invalid category lookup, either an id or a store and an internal id should be provided for Category: {0}"),

	CategoryStoreMismatch(100048, "The category store does not a store in another parameter for Category: {0}"),

	EmailTemplateNull(100049, "Invalid value null for EmailTemplate: {0}"),
	EmailTemplateNoLookup(100050, "Invalid email template lookup, an id should be provided for EmailTemplate: {0}"),
	EmailTemplateNotFound(100051, "Email template not found EmailTemplate: {0}"),

	InvalidActionCode(100055, "Invalid action code String: {0}"),

	GetCountriesNeedsStoreOrQuery(100101, "GetCountries call should either have a store or a query. To get all countries use * for the query: {0}"),

	GetStoresNeedsCountryOrQuery(100201, "GetStores call should either have a country or a query. To get all stores use * for the query: {0}"),

	// lookup service
	LookupApplicationNeedsInternalOrExternalId(100301,
			"LookupApplication should have at least one internal or external application id, you can also add multiple ids of each type: {0}"),

	InvalidCredentials(100400, "Invalid credentials, either the username (e-mail) or password are incorrect"),

	MissingPermissions(100500, "User does not have required permissions"),
	MissingRoles(100501, "User does not have required role"),
	MissingRolesAndOrPermissions(100502, "User does not have required roles and/or permissions"),

	DataAccountUserMissmatch(100601, "User does not own data account")

	;

	private int code;
	private String message;

	ApiError(int code, String message) {
		this.code = code;
		this.message = message;
	}

	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}

	public boolean isCode(int value) {
		return code == value;
	}

}
