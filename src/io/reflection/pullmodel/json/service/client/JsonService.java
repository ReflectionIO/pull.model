package io.reflection.pullmodel.json.service.client;

import java.io.IOException;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.RequestBuilder;
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
	protected AsyncHttpClient client = new AsyncHttpClient();

	public String getUrl() {
		return url;
	}

	public void setUrl(String value) {
		url = value;
	}

	protected void parseResponse(com.ning.http.client.Response response, Response outputParameter) throws HttpException, IOException {
		String responseText = null;
		if (response.getStatusCode() >= 200 && response.getStatusCode() < 300 && (responseText = response.getResponseBody()) != null
				&& !"".equals(responseText)) {
			outputParameter.fromJson(responseText);
		} else if (response.getStatusCode() >= 400)
			throw new HttpException(response.getStatusCode(), response.getStatusText());
	}

	protected <T> ListenableFuture<T> sendRequest(String action, Request input, AsyncCompletionHandler<T> callback) throws IOException {
		String requestData = "action=" + action + "&request=";

		RequestBuilder builder = new RequestBuilder("POST").setUrl(url);

		requestData += StringUtils.urlencode(JsonUtils.cleanJson(input.toJson().toString()));

		builder.setHeader("Content-Type", "application/x-www-form-urlencoded");
		builder.setBody(requestData.getBytes());

        return client.executeRequest(builder.build(), callback);
	}

	protected <T extends Response> void onCallStart(JsonService origin, String callName, Request input, ListenableFuture<T> requestHandle) {
	}

	protected void onCallSuccess(JsonService origin, String callName, Request input, Response output) {
	}

	protected void onCallFailure(JsonService origin, String callName, Request input, Throwable caught) {
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
	    client.close();
	    super.finalize();
	}
}