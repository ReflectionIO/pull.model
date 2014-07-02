//  
//  ICategoryService.java
//  reflection.io
//
//  Created by William Shakour on January 31, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.category;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Store;

import java.util.List;

import com.spacehopperstudios.service.IService;

public interface ICategoryService extends IService {
	/**
	 * @param id
	 * @return
	 */
	public Category getCategory(Long id) throws DataAccessException;

	/**
	 * @param category
	 * @return
	 */
	public Category addCategory(Category category) throws DataAccessException;

	/**
	 * @param category
	 * @return
	 */
	public Category updateCategory(Category category) throws DataAccessException;

	/**
	 * @param category
	 */
	public void deleteCategory(Category category) throws DataAccessException;

	/**
	 * 
	 * @param store
	 * @param pager
	 * @return
	 * @throws DataAccessException
	 */
	public List<Category> getStoreCategories(Store store, Pager pager) throws DataAccessException;

	/**
	 * 
	 * @param store
	 * @return
	 * @throws DataAccessException
	 */
	public Long getStoreCategoriesCount(Store store) throws DataAccessException;

	public List<Category> getParentCategories(Category parent, Pager pager) throws DataAccessException;

	public Long getParentCategoriesCount(Category parent) throws DataAccessException;

	/**
	 * @param store
	 * @return
	 */
	public Category getAllCategory(Store store) throws DataAccessException;

	/**
	 * @param store
	 * @param internalId
	 * @return
	 * @throws DataAccessException
	 */
	public Category getInternalIdCategory(Store store, Long internalId) throws DataAccessException;
	
	

}