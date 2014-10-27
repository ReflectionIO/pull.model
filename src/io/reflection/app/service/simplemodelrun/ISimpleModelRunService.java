//  
//  ISimpleModelRunService.java
//  reflection.io
//
//  Created by William Shakour on September 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.simplemodelrun;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.datatypes.shared.FeedFetch;
import io.reflection.app.datatypes.shared.SimpleModelRun;

import com.spacehopperstudios.service.IService;

public interface ISimpleModelRunService extends IService {

	/**
	 * Get simple model run
	 * 
	 * @param id
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun getSimpleModelRun(Long id) throws DataAccessException;

	/**
	 * Add simple model run
	 * 
	 * @param simpleModelRun
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun addSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * Update simple model run
	 * 
	 * @param simpleModelRun
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun updateSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * Delete simple model run
	 * 
	 * @param simpleModelRun
	 * @throws DataAccessException
	 */
	public void deleteSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * Get feed fetch simple model run
	 * 
	 * @param feedfetch
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun getFeedFetchSimpleModelRun(FeedFetch feedFetch) throws DataAccessException;

}