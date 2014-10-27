//  
//  SaleServiceFactory.java
//  reflection.io
//
//  Created by William Shakour on December 30, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.sale;

final class SaleServiceFactory {

	/**
	 * @return
	 */
	public static ISaleService createNewSaleService() {
		return new SaleService();
	}

}