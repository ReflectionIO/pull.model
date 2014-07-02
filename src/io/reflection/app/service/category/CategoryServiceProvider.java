//  
//  CategoryServiceProvider.java
//  reflection.io
//
//  Created by William Shakour on January 31, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.category;

import io.reflection.app.service.ServiceType;

import com.spacehopperstudios.service.ServiceDiscovery;

final public class CategoryServiceProvider {

	/**
	 * @return
	 */
	public static ICategoryService provide() {
		ICategoryService categoryService = null;

		if ((categoryService = (ICategoryService) ServiceDiscovery.getService(ServiceType.ServiceTypeCategory.toString())) == null) {
			categoryService = CategoryServiceFactory.createNewCategoryService();
			ServiceDiscovery.registerService(categoryService);
		}

		return categoryService;
	}

}