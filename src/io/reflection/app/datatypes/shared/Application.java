//  
//  Application.java
//  reflection.io
//
//  Created by William Shakour on September 17, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.datatypes.shared;

import java.util.Date;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class Application extends DataType {
	public String title;
	public String recommendedAge;
	public String artistName;
	public String sellerName;
	public String companyUrl;
	public String supportUrl;
	public String viewUrl;
	public String artworkUrlLarge;
	public String artworkUrlSmall;
	public Date itunesReleaseDate;
	public String copyright;
	public String description;
	public String version;
	public String itunesVersion;
	public Long downloadSize;
	public Boolean usesIap;

	@Override
	public JsonObject toJson() {
		JsonObject object = super.toJson();
		JsonElement jsonTitle = title == null ? JsonNull.INSTANCE : new JsonPrimitive(title);
		object.add("title", jsonTitle);
		JsonElement jsonRecommendedAge = recommendedAge == null ? JsonNull.INSTANCE : new JsonPrimitive(recommendedAge);
		object.add("recommendedAge", jsonRecommendedAge);
		JsonElement jsonArtistName = artistName == null ? JsonNull.INSTANCE : new JsonPrimitive(artistName);
		object.add("artistName", jsonArtistName);
		JsonElement jsonSellerName = sellerName == null ? JsonNull.INSTANCE : new JsonPrimitive(sellerName);
		object.add("sellerName", jsonSellerName);
		JsonElement jsonCompanyUrl = companyUrl == null ? JsonNull.INSTANCE : new JsonPrimitive(companyUrl);
		object.add("companyUrl", jsonCompanyUrl);
		JsonElement jsonSupportUrl = supportUrl == null ? JsonNull.INSTANCE : new JsonPrimitive(supportUrl);
		object.add("supportUrl", jsonSupportUrl);
		JsonElement jsonViewUrl = viewUrl == null ? JsonNull.INSTANCE : new JsonPrimitive(viewUrl);
		object.add("viewUrl", jsonViewUrl);
		JsonElement jsonArtworkUrlLarge = artworkUrlLarge == null ? JsonNull.INSTANCE : new JsonPrimitive(artworkUrlLarge);
		object.add("artworkUrlLarge", jsonArtworkUrlLarge);
		JsonElement jsonArtworkUrlSmall = artworkUrlSmall == null ? JsonNull.INSTANCE : new JsonPrimitive(artworkUrlSmall);
		object.add("artworkUrlSmall", jsonArtworkUrlSmall);
		JsonElement jsonItunesReleaseDate = itunesReleaseDate == null ? JsonNull.INSTANCE : new JsonPrimitive(itunesReleaseDate.getTime());
		object.add("itunesReleaseDate", jsonItunesReleaseDate);
		JsonElement jsonCopyright = copyright == null ? JsonNull.INSTANCE : new JsonPrimitive(copyright);
		object.add("copyright", jsonCopyright);
		JsonElement jsonDescription = description == null ? JsonNull.INSTANCE : new JsonPrimitive(description);
		object.add("description", jsonDescription);
		JsonElement jsonVersion = version == null ? JsonNull.INSTANCE : new JsonPrimitive(version);
		object.add("version", jsonVersion);
		JsonElement jsonItunesVersion = itunesVersion == null ? JsonNull.INSTANCE : new JsonPrimitive(itunesVersion);
		object.add("itunesVersion", jsonItunesVersion);
		JsonElement jsonDownloadSize = downloadSize == null ? JsonNull.INSTANCE : new JsonPrimitive(downloadSize);
		object.add("downloadSize", jsonDownloadSize);
		JsonElement jsonUsesIap = usesIap == null ? JsonNull.INSTANCE : new JsonPrimitive(usesIap);
		object.add("usesIap", jsonUsesIap);
		return object;
	}

	@Override
	public void fromJson(JsonObject jsonObject) {
		super.fromJson(jsonObject);
		if (jsonObject.has("title")) {
			JsonElement jsonTitle = jsonObject.get("title");
			if (jsonTitle != null) {
				title = jsonTitle.getAsString();
			}
		}
		if (jsonObject.has("recommendedAge")) {
			JsonElement jsonRecommendedAge = jsonObject.get("recommendedAge");
			if (jsonRecommendedAge != null) {
				recommendedAge = jsonRecommendedAge.getAsString();
			}
		}
		if (jsonObject.has("artistName")) {
			JsonElement jsonArtistName = jsonObject.get("artistName");
			if (jsonArtistName != null) {
				artistName = jsonArtistName.getAsString();
			}
		}
		if (jsonObject.has("sellerName")) {
			JsonElement jsonSellerName = jsonObject.get("sellerName");
			if (jsonSellerName != null) {
				sellerName = jsonSellerName.getAsString();
			}
		}
		if (jsonObject.has("companyUrl")) {
			JsonElement jsonCompanyUrl = jsonObject.get("companyUrl");
			if (jsonCompanyUrl != null) {
				companyUrl = jsonCompanyUrl.getAsString();
			}
		}
		if (jsonObject.has("supportUrl")) {
			JsonElement jsonSupportUrl = jsonObject.get("supportUrl");
			if (jsonSupportUrl != null) {
				supportUrl = jsonSupportUrl.getAsString();
			}
		}
		if (jsonObject.has("viewUrl")) {
			JsonElement jsonViewUrl = jsonObject.get("viewUrl");
			if (jsonViewUrl != null) {
				viewUrl = jsonViewUrl.getAsString();
			}
		}
		if (jsonObject.has("artworkUrlLarge")) {
			JsonElement jsonArtworkUrlLarge = jsonObject.get("artworkUrlLarge");
			if (jsonArtworkUrlLarge != null) {
				artworkUrlLarge = jsonArtworkUrlLarge.getAsString();
			}
		}
		if (jsonObject.has("artworkUrlSmall")) {
			JsonElement jsonArtworkUrlSmall = jsonObject.get("artworkUrlSmall");
			if (jsonArtworkUrlSmall != null) {
				artworkUrlSmall = jsonArtworkUrlSmall.getAsString();
			}
		}
		if (jsonObject.has("itunesReleaseDate")) {
			JsonElement jsonItunesReleaseDate = jsonObject.get("itunesReleaseDate");
			if (jsonItunesReleaseDate != null) {
				itunesReleaseDate = new Date(jsonItunesReleaseDate.getAsLong());
			}
		}
		if (jsonObject.has("copyright")) {
			JsonElement jsonCopyright = jsonObject.get("copyright");
			if (jsonCopyright != null) {
				copyright = jsonCopyright.getAsString();
			}
		}
		if (jsonObject.has("description")) {
			JsonElement jsonDescription = jsonObject.get("description");
			if (jsonDescription != null) {
				description = jsonDescription.getAsString();
			}
		}
		if (jsonObject.has("version")) {
			JsonElement jsonVersion = jsonObject.get("version");
			if (jsonVersion != null) {
				version = jsonVersion.getAsString();
			}
		}
		if (jsonObject.has("itunesVersion")) {
			JsonElement jsonItunesVersion = jsonObject.get("itunesVersion");
			if (jsonItunesVersion != null) {
				itunesVersion = jsonItunesVersion.getAsString();
			}
		}
		if (jsonObject.has("downloadSize")) {
			JsonElement jsonDownloadSize = jsonObject.get("downloadSize");
			if (jsonDownloadSize != null) {
				downloadSize = Long.valueOf(jsonDownloadSize.getAsLong());
			}
		}
		if (jsonObject.has("usesIap")) {
			JsonElement jsonUsesIap = jsonObject.get("usesIap");
			if (jsonUsesIap != null) {
				usesIap = Boolean.valueOf(jsonUsesIap.getAsBoolean());
			}
		}
	}
}