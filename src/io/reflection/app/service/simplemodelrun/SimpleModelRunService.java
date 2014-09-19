//  
//  SimpleModelRunService.java
//  reflection.io
//
//  Created by William Shakour on September 5, 2014.
//  Copyrights © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.simplemodelrun;

import static com.spacehopperstudios.utility.StringUtils.addslashes;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.FormType;
import io.reflection.app.datatypes.shared.SimpleModelRun;
import io.reflection.app.datatypes.shared.Store;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;
import io.reflection.app.service.feedfetch.FeedFetchServiceProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

final class SimpleModelRunService implements ISimpleModelRunService {
	public String getName() {
		return ServiceType.ServiceTypeSimpleModelRun.toString();
	}

	@Override
	public SimpleModelRun getSimpleModelRun(Long id) throws DataAccessException {
		SimpleModelRun simpleModelRun = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection simpleModelRunConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		String getSimpleModelRunQuery = String.format("SELECT * FROM `simplemodelrun` WHERE `deleted`='n' AND `id`=%d LIMIT 1", id.longValue());
		try {
			simpleModelRunConnection.connect();
			simpleModelRunConnection.executeQuery(getSimpleModelRunQuery);

			if (simpleModelRunConnection.fetchNextRow()) {
				simpleModelRun = toSimpleModelRun(simpleModelRunConnection);
			}
		} finally {
			if (simpleModelRunConnection != null) {
				simpleModelRunConnection.disconnect();
			}
		}
		return simpleModelRun;
	}

	/**
	 * To simpleModelRun
	 * 
	 * @param connection
	 * @return
	 * @throws DataAccessException
	 */
	private SimpleModelRun toSimpleModelRun(Connection connection) throws DataAccessException {
		SimpleModelRun simpleModelRun = new SimpleModelRun();

		simpleModelRun.id = connection.getCurrentRowLong("id");
		simpleModelRun.created = connection.getCurrentRowDateTime("created");
		simpleModelRun.deleted = connection.getCurrentRowString("deleted");

		simpleModelRun.country = connection.getCurrentRowString("country");
		simpleModelRun.store = connection.getCurrentRowString("store");

		simpleModelRun.category = new Category();
		simpleModelRun.category.id = connection.getCurrentRowLong("categoryid");

		simpleModelRun.code = connection.getCurrentRowLong("code");
		simpleModelRun.form = FormType.fromString(connection.getCurrentRowString("form"));
		simpleModelRun.a = connection.getCurrentRowDouble("a");
		simpleModelRun.b = connection.getCurrentRowDouble("b");

		return simpleModelRun;
	}

	@Override
	public SimpleModelRun addSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException {
		SimpleModelRun addedSimpleModelRun = null;

		final String addSimpleModelRunQuery = String.format(
				"INSERT INTO `simplemodelrun` (`country`,`store`,`categoryid`,`code`,`form`,`a`,`b`) VALUES ('%s','%s',%d,%d,'%s',%f,%f);",
				addslashes(simpleModelRun.country), addslashes(simpleModelRun.store), simpleModelRun.category.id.longValue(), simpleModelRun.code.longValue(),
				simpleModelRun.form.toString(), simpleModelRun.a.doubleValue(), simpleModelRun.b.doubleValue());

		Connection simpleModelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		try {
			simpleModelRunConnection.connect();
			simpleModelRunConnection.executeQuery(addSimpleModelRunQuery);

			if (simpleModelRunConnection.getAffectedRowCount() > 0) {
				addedSimpleModelRun = getSimpleModelRun(Long.valueOf(simpleModelRunConnection.getInsertedId()));

				if (addedSimpleModelRun == null) {
					addedSimpleModelRun = simpleModelRun;
					addedSimpleModelRun.id = Long.valueOf(simpleModelRunConnection.getInsertedId());
				}
			}
		} finally {
			if (simpleModelRunConnection != null) {
				simpleModelRunConnection.disconnect();
			}
		}

		return addedSimpleModelRun;
	}

	@Override
	public SimpleModelRun updateSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException {
		SimpleModelRun updatedSimpleModelRun = null;

		final String updateSimpleModelRunQuery = String.format(
				"UPDATE `simplemodelrun` SET `country`='%s',`store`='%s',`categoryid`=%d,`code`=%d,`form`='%s',`a`=%f,`b`=%f WHERE `id`=%d AND `deleted`='n';",
				addslashes(simpleModelRun.country), addslashes(simpleModelRun.store), simpleModelRun.category.id.longValue(), simpleModelRun.code.longValue(),
				simpleModelRun.form.toString(), simpleModelRun.a.doubleValue(), simpleModelRun.b.doubleValue(), simpleModelRun.id.longValue());

		Connection simpleSimpleModelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		try {
			simpleSimpleModelRunConnection.connect();
			simpleSimpleModelRunConnection.executeQuery(updateSimpleModelRunQuery);

			if (simpleSimpleModelRunConnection.getAffectedRowCount() > 0) {
				updatedSimpleModelRun = getSimpleModelRun(simpleModelRun.id);
			} else {
				updatedSimpleModelRun = simpleModelRun;
			}

		} finally {
			if (simpleSimpleModelRunConnection != null) {
				simpleSimpleModelRunConnection.disconnect();
			}
		}

		return updatedSimpleModelRun;
	}

	@Override
	public void deleteSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.simplemodelrun.ISimpleModelRunService#getGatherCodeSimpleModelRun(io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Store, io.reflection.app.datatypes.shared.FormType, io.reflection.app.datatypes.shared.Category, java.lang.Long)
	 */
	@Override
	public SimpleModelRun getGatherCodeSimpleModelRun(Country country, Store store, FormType form, Category category, Long code) throws DataAccessException {
		SimpleModelRun simpleModelRun = null;

		final String getGatherCodeSimpleModelRunQuery = String.format(
				"SELECT * FROM `simplemodelrun` WHERE `store`='%s' AND `country`='%s' AND `categoryid`=%d AND `form`='%s' AND `code`=%d",
				addslashes(store.a3Code), addslashes(country.a2Code), category.id.longValue(), form.toString(), code.longValue());

		Connection simpleModelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		try {
			simpleModelRunConnection.connect();
			simpleModelRunConnection.executeQuery(getGatherCodeSimpleModelRunQuery);

			if (simpleModelRunConnection.fetchNextRow()) {
				simpleModelRun = toSimpleModelRun(simpleModelRunConnection);
			}
		} finally {
			if (simpleModelRunConnection != null) {
				simpleModelRunConnection.disconnect();
			}
		}

		return simpleModelRun;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.simplemodelrun.ISimpleModelRunService#getSimpleModelRun(io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Store, io.reflection.app.datatypes.shared.FormType, io.reflection.app.datatypes.shared.Category, java.util.Date,
	 * java.util.Date)
	 */
	@Override
	public SimpleModelRun getSimpleModelRun(Country country, Store store, FormType form, Category category, Date start, Date end) throws DataAccessException {
		SimpleModelRun simpleModelRun = null;

		Long code = FeedFetchServiceProvider.provide().getGatherCode(country, store, start, end);

		String getSimpleModelRunQuery = String
				.format("SELECT * FROM `simplemodelrun` WHERE CAST(`country` AS BINARY)=CAST('%s' AS BINARY) AND CAST(`store` AS BINARY)=CAST('%s' AS BINARY) AND `categoryid`=%d AND `form`='%s' AND `code`=%d `deleted`='n' ORDER BY `id` DESC LIMIT 1",
						addslashes(country.a2Code), addslashes(store.a3Code), category.id.longValue(), form.toString(), code.longValue());

		Connection simpleModelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		try {
			simpleModelRunConnection.connect();
			simpleModelRunConnection.executeQuery(getSimpleModelRunQuery);

			if (simpleModelRunConnection.fetchNextRow()) {
				simpleModelRun = toSimpleModelRun(simpleModelRunConnection);
			}
		} finally {
			if (simpleModelRunConnection != null) {
				simpleModelRunConnection.disconnect();
			}
		}

		return simpleModelRun;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.simplemodelrun.ISimpleModelRunService#getDateSimpleModelRunBatch(io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Store, io.reflection.app.datatypes.shared.FormType, io.reflection.app.datatypes.shared.Category, java.util.Collection)
	 */
	@Override
	public List<SimpleModelRun> getDateSimpleModelRunBatch(Country country, Store store, FormType form, Category category, Collection<Date> dates)
			throws DataAccessException {
		return new ArrayList<SimpleModelRun>();
	}

}