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
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.FormType;
import io.reflection.app.datatypes.shared.SimpleModelRun;
import io.reflection.app.datatypes.shared.Store;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.spacehopperstudios.service.IService;

public interface ISimpleModelRunService extends IService {
	/**
	 * @param id
	 * @return
	 */
	public SimpleModelRun getSimpleModelRun(Long id) throws DataAccessException;

	/**
	 * @param simpleModelRun
	 * @return
	 */
	public SimpleModelRun addSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * @param simpleModelRun
	 * @return
	 */
	public SimpleModelRun updateSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * @param simpleModelRun
	 */
	public void deleteSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException;

	/**
	 * Get gather code simple model run
	 * 
	 * @param country
	 * @param store
	 * @param form
	 * @param category
	 * @param code
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun getGatherCodeSimpleModelRun(Country country, Store store, FormType form, Category category, Long code) throws DataAccessException;

	/**
	 * Get simple model run
	 * 
	 * @param country
	 * @param store
	 * @param form
	 * @param category
	 * @param start
	 * @param end
	 * @return
	 * @throws DataAccessException
	 */
	public SimpleModelRun getSimpleModelRun(Country country, Store store, FormType form, Category category, Date start, Date end) throws DataAccessException;

	/**
	 * Get date simple model run batch
	 * 
	 * @param country
	 * @param store
	 * @param form
	 * @param category
	 * @param dates
	 * @return
	 * @throws DataAccessException
	 */
	public List<SimpleModelRun> getDateSimpleModelRunBatch(Country country, Store store, FormType form, Category category, Collection<Date> dates)
			throws DataAccessException;

}