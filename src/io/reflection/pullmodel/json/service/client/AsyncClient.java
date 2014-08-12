//
//  AsyncClient.java
//  pull.model
//
//  Created by (William Shakour (billy1380)) on 12 Aug 2014.
//  Copyright © 2014 Reflection.io Ltd. All rights reserved.
//
package io.reflection.pullmodel.json.service.client;

import com.ning.http.client.AsyncHttpClient;

/**
 * @author William Shakour (billy1380)
 * 
 */
class AsyncClient {
    protected static AsyncHttpClient client = new AsyncHttpClient();

    public static AsyncHttpClient get() {
        return client;
    }

}
