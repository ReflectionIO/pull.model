//  
//  SaleServiceProvider.java
//  reflection.io
//
//  Created by William Shakour on December 30, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.sale;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class SaleServiceProvider {

	/**
	 * @return
	 */
	public static ISaleService provide() {
		ISaleService saleService = null;

		if ((saleService = (ISaleService) ServiceDiscovery.getService(ServiceType.ServiceTypeSale.toString())) == null) {
			saleService = SaleServiceFactory.createNewSaleService();
			ServiceDiscovery.registerService(saleService);
		}

		return saleService;
	}

}