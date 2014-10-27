//  
//  IFeedFetchService.java
//  storedata
//
//  Created by William Shakour on September 5, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//

package io.reflection.app.service.feedfetch;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.FeedFetch;
import io.reflection.app.datatypes.shared.Store;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.spacehopperstudios.service.IService;

public interface IFeedFetchService extends IService {
	/**
	 * @param id
	 * @return
	 */
	public FeedFetch getFeedFetch(Long id) throws DataAccessException;

	/**
	 * @param feedFetch
	 * @return
	 */
	public FeedFetch addFeedFetch(FeedFetch feedFetch) throws DataAccessException;

	/**
	 * @param feedFetch
	 * @return
	 */
	public FeedFetch updateFeedFetch(FeedFetch feedFetch) throws DataAccessException;

	/**
	 * @param feedFetch
	 */
	public void deleteFeedFetch(FeedFetch feedFetch) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @param pager
	 * @return
	 */
	public List<FeedFetch> getFeedFetches(Country country, Store store, Collection<String> types, Pager pager) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @return
	 */
	public Long getFeedFetchesCount(Country country, Store store, Collection<String> types) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @param pager
	 * @return
	 */
	public List<FeedFetch> getIngestedFeedFetches(Country country, Store store, Collection<String> types, Pager pager) throws DataAccessException;

	/**
	 * 
	 * @param store
	 * @param country
	 * @param listType
	 * @return
	 */
	public Long getIngestedFeedFetchesCount(Country country, Store store, Collection<String> listType) throws DataAccessException;

	/**
	 * @param store
	 * @param country
	 * @param types
	 * @param pager
	 * @return
	 */
	public List<FeedFetch> getUningestedFeedFetches(Country country, Store store, Collection<String> types, Pager pager) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @return
	 */
	public Long getUningestedFeedFetchesCount(Country country, Store store, Collection<String> types) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param listType
	 * @param code
	 * @return
	 */
	public List<Long> getIngestableFeedFetchIds(Country country, Store store, String listType, Long code) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @param code
	 * @return
	 */
	public Boolean isReadyToModel(Country country, Store store, Collection<String> types, Long code) throws DataAccessException;

	/**
	 * 
	 * @param country
	 * @param store
	 * @param types
	 * @param code
	 * @return
	 */
	public List<FeedFetch> getGatherCodeFeedFetches(Country country, Store store, Collection<String> types, Long code) throws DataAccessException;

	/**
	 * Gets a feed fetch code instead of an guid
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	public Long getCode() throws DataAccessException;
	
	/**
	 * Gets the gather for the feed fetch
	 * @param country
	 * @param store
	 * @param after
	 * @param before
	 * @return
	 * @throws DataAccessException
	 */
	public Long getGatherCode(Country country, Store store, Date after, Date before) throws DataAccessException;
	
	/**
	 * 
	 * @param country
	 * @param store
	 * @param category
	 * @param listType
	 * @param code
	 * @return
	 * @throws DataAccessException
	 */
	public FeedFetch getListTypeCodeFeedFetch(Country country, Store store, Category category, String listType, Long code) throws DataAccessException;

}