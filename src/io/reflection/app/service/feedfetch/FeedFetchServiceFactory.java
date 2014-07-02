//  
//  FeedFetchServiceFactory.java
//  storedata
//
//  Created by William Shakour on September 5, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.feedfetch;

final class FeedFetchServiceFactory {

	/**
	 * @return
	 */
	public static IFeedFetchService createNewFeedFetchService() {
		return new FeedFetchService();
	}

}