//  
//  CategoryServiceFactory.java
//  reflection.io
//
//  Created by William Shakour on January 31, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.category;

final class CategoryServiceFactory {

	/**
	 * @return
	 */
	public static ICategoryService createNewCategoryService() {
		return new CategoryService();
	}

}