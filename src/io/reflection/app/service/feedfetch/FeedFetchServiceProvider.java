//  
//  FeedFetchServiceProvider.java
//  storedata
//
//  Created by William Shakour on September 5, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.feedfetch;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class FeedFetchServiceProvider {

	/**
	 * @return
	 */
	public static IFeedFetchService provide() {
		IFeedFetchService feedFetchService = null;

		if ((feedFetchService = (IFeedFetchService) ServiceDiscovery.getService(ServiceType.ServiceTypeFeedFetch.toString())) == null) {
			feedFetchService = FeedFetchServiceFactory.createNewFeedFetchService();
			ServiceDiscovery.registerService(feedFetchService);
		}

		return feedFetchService;
	}

}