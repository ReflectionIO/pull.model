//  
//  admin/AdminService.java
//  reflection.io
//
//  Created by William Shakour on October 9, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.pullmodel.json.service.client;

import io.reflection.app.api.admin.shared.call.TriggerPredictRequest;
import io.reflection.app.api.admin.shared.call.TriggerPredictResponse;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpExecutionAware;

import com.willshex.gson.json.service.client.HttpException;

/**
 * This is a subset of the full admin service client specifically for the pull.model project
 * 
 * @author William Shakour (billy1380)
 *
 */
public final class AdminService extends JsonService {

	public static final String AdminMethodTriggerPredict = "TriggerPredict";

	public HttpExecutionAware triggerPredict(final TriggerPredictRequest input, final AsyncCallback<TriggerPredictResponse> output) {
		HttpExecutionAware handle = null;
		try {
			handle = sendRequest(AdminMethodTriggerPredict, input, new ResponseHandler<TriggerPredictResponse>() {
				@Override
				public TriggerPredictResponse handleResponse(HttpResponse response) throws ClientProtocolException, IOException {

					TriggerPredictResponse outputParameter = new TriggerPredictResponse();
					try {
						parseResponse(response, outputParameter);
						output.onSuccess(outputParameter);
						onCallSuccess(AdminService.this, AdminMethodTriggerPredict, input, outputParameter);
					} catch (HttpException exception) {
						output.onFailure(exception);
						onCallFailure(AdminService.this, AdminMethodTriggerPredict, input, exception);
					}

					return outputParameter;
				}
			});
			onCallStart(AdminService.this, AdminMethodTriggerPredict, input, handle);
		} catch (IOException exception) {
			output.onFailure(exception);
			onCallFailure(AdminService.this, AdminMethodTriggerPredict, input, exception);
		}
		return handle;
	}
}