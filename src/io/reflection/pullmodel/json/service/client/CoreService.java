//  
//  core/CoreService.java
//  storedata
//
//  Created by William Shakour on October 2, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.pullmodel.json.service.client;

import io.reflection.app.api.core.shared.call.LoginRequest;
import io.reflection.app.api.core.shared.call.LoginResponse;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpExecutionAware;

import com.willshex.gson.json.service.client.HttpException;

/**
 * This is a subset of the full core service client specifically for the pull.model project
 * 
 * @author William Shakour (billy1380)
 *
 */
public final class CoreService extends JsonService {

	public static final String CoreMethodLogin = "Login";

	public HttpExecutionAware login(final LoginRequest input, final AsyncCallback<LoginResponse> output) {
		HttpExecutionAware handle = null;
		try {
			handle = sendRequest(CoreMethodLogin, input, new ResponseHandler<LoginResponse>() {

				@Override
				public LoginResponse handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
					LoginResponse outputParameter = new LoginResponse();
					try {
						parseResponse(response, outputParameter);
						output.onSuccess(outputParameter);
						onCallSuccess(CoreService.this, CoreMethodLogin, input, outputParameter);
					} catch (HttpException exception) {
						output.onFailure(exception);
						onCallFailure(CoreService.this, CoreMethodLogin, input, exception);
					}

					return outputParameter;
				}
			});
			onCallStart(CoreService.this, CoreMethodLogin, input, handle);
		} catch (IOException exception) {
			output.onFailure(exception);
			onCallFailure(CoreService.this, CoreMethodLogin, input, exception);
		}
		return handle;
	}
}