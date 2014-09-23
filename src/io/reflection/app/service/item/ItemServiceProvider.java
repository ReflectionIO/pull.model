//  
//  ItemServiceProvider.java
//  storedata
//
//  Created by William Shakour on 18 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//

package io.reflection.app.service.item;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class ItemServiceProvider {

	/**
	 * @return
	 */
	public static IItemService provide() {
		IItemService itemService = null;

		if ((itemService = (IItemService) ServiceDiscovery.getService(ServiceType.ServiceTypeItem.toString())) == null) {
			itemService = ItemServiceFactory.createNewItemService();
			ServiceDiscovery.registerService(itemService);
		}

		return itemService;
	}

}