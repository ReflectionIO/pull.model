//
//  ModellerFactory.java
//  storedata
//
//  Created by William Shakour (billy1380) on 15 Oct 2013.
//  Copyright © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package io.reflection.app.modellers;

/**
 * @author billy1380
 * 
 */
public class ModellerFactory {

	/**
	 * @param store
	 * @return
	 */
	public static Modeller getModellerForStore(String store) {
		Modeller modeller = null;

		if ("ios".equals(store.toLowerCase())) {
			// ios store
			modeller = new ModellerIOS();
		} else if ("azn".equals(store.toLowerCase())) {
			// amazon store
		} else if ("gpl".equals(store.toLowerCase())) {
			// google play store
		}

		return modeller;
	}

}
