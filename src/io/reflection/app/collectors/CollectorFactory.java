//  
//  CollectorFactory.java
//  storedata
//
//  Created by William Shakour on 30 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.collectors;


/**
 * @author billy1380
 *
 */
public class CollectorFactory {

	/**
	 * @param store
	 * @return
	 */
	public static Collector getCollectorForStore(String store) {
		Collector collector = null;
		
		if ("ios".equals(store.toLowerCase())) {
			// ios store
			collector = new CollectorIOS();
		} else if ("azn".equals(store.toLowerCase())) {
			// amazon store
			collector = new CollectorAmazon();
		} else if ("gpl".equals(store.toLowerCase())) {
			// google play store
		}
		
		return collector;
	}

}
