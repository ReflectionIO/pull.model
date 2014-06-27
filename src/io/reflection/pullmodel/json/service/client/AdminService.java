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

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.ListenableFuture;
import com.willshex.gson.json.service.client.HttpException;

/**
 * This is a subset of the full admin service client specifically for the pull.model project
 * @author William Shakour (billy1380)
 *
 */
public final class AdminService extends JsonService {

	public static final String AdminMethodTriggerPredict = "TriggerPredict";

	public ListenableFuture<TriggerPredictResponse> triggerPredict(final TriggerPredictRequest input, final AsyncCallback<TriggerPredictResponse> output) {
		ListenableFuture<TriggerPredictResponse> handle = null;
		try {
			handle = sendRequest(AdminMethodTriggerPredict, input, new AsyncCompletionHandler<TriggerPredictResponse>() {
				@Override
				public TriggerPredictResponse onCompleted(com.ning.http.client.Response response) throws Exception {
					try {
						TriggerPredictResponse outputParameter = new TriggerPredictResponse();
						parseResponse(response, outputParameter);
						output.onSuccess(outputParameter);
						onCallSuccess(AdminService.this, AdminMethodTriggerPredict, input, outputParameter);
					} catch (HttpException exception) {
						output.onFailure(exception);
						onCallFailure(AdminService.this, AdminMethodTriggerPredict, input, exception);
					}
					return null;
				}

				@Override
				public void onThrowable(Throwable exception) {
					super.onThrowable(exception);
					output.onFailure(exception);
					onCallFailure(AdminService.this, AdminMethodTriggerPredict, input, exception);
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