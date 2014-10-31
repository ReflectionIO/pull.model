package io.reflection.pullmodel.json.service.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpExecutionAware;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.spacehopperstudios.utility.JsonUtils;
import com.spacehopperstudios.utility.StringUtils;
import com.willshex.gson.json.service.client.HttpException;
import com.willshex.gson.json.service.shared.Request;
import com.willshex.gson.json.service.shared.Response;

public class JsonService {
	public interface AsyncCallback<T> {
		void onFailure(Throwable caught);

		void onSuccess(T result);
	}

	protected String url;
	protected HttpClient client = HttpClientBuilder.create().build();

	public String getUrl() {
		return url;
	}

	public void setUrl(String value) {
		url = value;
	}

	protected void parseResponse(HttpResponse response, Response outputParameter) throws HttpException, IOException {
		String responseText = null;
		if (response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300
				&& (responseText = getResponseBody(response)) != null && !"".equals(responseText)) {
			outputParameter.fromJson(responseText);
		} else if (response.getStatusLine().getStatusCode() >= 400)
			throw new HttpException(response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
	}

	protected String getResponseBody(HttpResponse response) throws IOException {
		StringBuffer body = new StringBuffer();

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line;
			while ((line = reader.readLine()) != null) {
				body.append(line);
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}

		return body.toString();
	}

	protected <T> HttpExecutionAware sendRequest(String action, Request input, ResponseHandler<T> callback) throws IOException {
		String requestData = "action=" + action + "&request=";

		HttpPost request = new HttpPost(url);

		requestData += StringUtils.urlencode(JsonUtils.cleanJson(input.toJson().toString()));

		request.setHeader("Content-Type", "application/x-www-form-urlencoded");
		request.setEntity(new StringEntity(requestData));

		// T response =
		client.execute(request, callback);

		return request;
	}

	protected <T extends Response> void onCallStart(JsonService origin, String callName, Request input, HttpExecutionAware output) {}

	protected void onCallSuccess(JsonService origin, String callName, Request input, Response output) {}

	protected void onCallFailure(JsonService origin, String callName, Request input, Throwable caught) {}
}