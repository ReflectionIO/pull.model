//  
//  IItemService.java
//  storedata
//
//  Created by William Shakour on 18 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//

package io.reflection.app.service.item;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Item;

import java.util.Collection;
import java.util.List;

import com.spacehopperstudios.service.IService;

public interface IItemService extends IService {
	/**
	 * @param id
	 * @return
	 */
	public Item getItem(Long id) throws DataAccessException;

	/**
	 * @param item
	 * @return
	 */
	public Item addItem(Item item) throws DataAccessException;

	/**
	 * @param item
	 * @return
	 */
	public Item updateItem(Item item) throws DataAccessException;

	/**
	 * @param item
	 */
	public void deleteItem(Item item) throws DataAccessException;

	/**
	 * @param externalId
	 * @return
	 */
	public Item getExternalIdItem(String externalId) throws DataAccessException;

	/**
	 * @param internalId
	 * @return
	 */
	public Item getInternalIdItem(String internalId) throws DataAccessException;

	/**
	 * @param internalId
	 * @param includeDuplicates
	 * @return
	 */
	public List<Item> getInternalIdItemAndDuplicates(String internalId) throws DataAccessException;

	/**
	 * @param itemIds
	 * @return
	 */
	public List<Item> getExternalIdItemBatch(Collection<String> itemIds) throws DataAccessException;

	/**
	 * @param items
	 * @return
	 */
	public Long addItemsBatch(Collection<Item> items) throws DataAccessException;

	/**
	 * @param query
	 * @param pager
	 * @return
	 */
	public List<Item> searchItems(String query, Pager pager) throws DataAccessException;

	/**
	 * @param query
	 * @return
	 * @throws DataAccessException
	 */
	public Long searchItemsCount(String query) throws DataAccessException;

	/**
	 * @param itemIds
	 * @return
	 */
	public List<Item> getInternalIdItemBatch(Collection<String> itemIds) throws DataAccessException;

	/**
	 * @param pager
	 * @return
	 */
	public List<Item> getItems(Pager pager) throws DataAccessException;

	/**
	 * @return
	 */
	public Long getItemsCount() throws DataAccessException;

	/**
	 * @param infinitePager
	 * @return
	 */
	public List<String> getDuplicateItemsInternalId(Pager infinitePager) throws DataAccessException;

	/**
	 * Get propertyless item count
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	public Long getPropertylessItemsCount() throws DataAccessException;

	/**
	 * Get propertyless items
	 * 
	 * @param pager
	 * @return
	 * @throws DataAccessException
	 */
	public List<Item> getPropertylessItems(Pager pager) throws DataAccessException;

	/**
	 * Get propertyless item ids
	 * 
	 * @param pager
	 * @return
	 * @throws DataAccessException
	 */
	public List<Long> getPropertylessItemIds(Pager pager) throws DataAccessException;

}