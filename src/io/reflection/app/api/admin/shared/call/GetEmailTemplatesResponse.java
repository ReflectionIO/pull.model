//  
//  GetEmailTemplatesResponse.java
//  reflection.io
//
//  Created by William Shakour on February 20, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.api.admin.shared.call;

import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.EmailTemplate;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.reflection.app.api.shared.datatypes.Response;

public class GetEmailTemplatesResponse extends Response {
public List<EmailTemplate> templates;
public Pager pager;@Override
public JsonObject toJson() {
JsonObject object = super.toJson();
JsonElement jsonTemplates = JsonNull.INSTANCE;
if (templates != null) {
jsonTemplates = new JsonArray();
for (int i = 0; i < templates.size(); i++) {
JsonElement jsonTemplatesItem = templates.get(i) == null ? JsonNull.INSTANCE : templates.get(i).toJson();
((JsonArray)jsonTemplates).add(jsonTemplatesItem);
}
}
object.add("templates", jsonTemplates);
JsonElement jsonPager = pager == null ? JsonNull.INSTANCE : pager.toJson();
object.add("pager", jsonPager);
return object;
}@Override
public void fromJson(JsonObject jsonObject) {
super.fromJson(jsonObject);
if (jsonObject.has("templates")) {
JsonElement jsonTemplates = jsonObject.get("templates");
if (jsonTemplates != null) {
templates = new ArrayList<EmailTemplate>();
EmailTemplate item = null;
for (int i = 0; i < jsonTemplates.getAsJsonArray().size(); i++) {
if (jsonTemplates.getAsJsonArray().get(i) != null) {
(item = new EmailTemplate()).fromJson(jsonTemplates.getAsJsonArray().get(i).getAsJsonObject());
templates.add(item);
}
}
}
}

if (jsonObject.has("pager")) {
JsonElement jsonPager = jsonObject.get("pager");
if (jsonPager != null) {
pager = new Pager();
pager.fromJson(jsonPager.getAsJsonObject());
}
}
}
}