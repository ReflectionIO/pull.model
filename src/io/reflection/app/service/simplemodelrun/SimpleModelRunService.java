//  
//  SimpleModelRunService.java
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
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;

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

		simpleModelRun.feedFetch = new FeedFetch();
		simpleModelRun.feedFetch.id = connection.getCurrentRowLong("country");

		simpleModelRun.a = connection.getCurrentRowDouble("a");
		simpleModelRun.b = connection.getCurrentRowDouble("b");

		return simpleModelRun;
	}

	@Override
	public SimpleModelRun addSimpleModelRun(SimpleModelRun simpleModelRun) throws DataAccessException {
		SimpleModelRun addedSimpleModelRun = null;

		final String addSimpleModelRunQuery = String.format("INSERT INTO `simplemodelrun` (`feedfetchid`,`a`,`b`) VALUES (%d,%f,%f);",
				simpleModelRun.feedFetch.id.longValue(), simpleModelRun.a.doubleValue(), simpleModelRun.b.doubleValue());

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

		final String updateSimpleModelRunQuery = String.format("UPDATE `simplemodelrun` SET `feedfetchid`=%d,`a`=%f,`b`=%f WHERE `id`=%d AND `deleted`='n';",
				simpleModelRun.feedFetch.id, simpleModelRun.a.doubleValue(), simpleModelRun.b.doubleValue(), simpleModelRun.id.longValue());

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
	 * @see io.reflection.app.service.simplemodelrun.ISimpleModelRunService#getFeedFetchSimpleModelRun(io.reflection.app.datatypes.shared.FeedFetch)
	 */
	@Override
	public SimpleModelRun getFeedFetchSimpleModelRun(FeedFetch feedFetch) throws DataAccessException {
		SimpleModelRun simpleModelRun = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection simpleModelRunConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeSimpleModelRun.toString());

		String getFeedFetchSimpleModelRunQuery = String.format("SELECT * FROM `simplemodelrun` WHERE `feedfetchid`=%d AND `deleted`='n' LIMIT 1",
				feedFetch.id.longValue());
		try {
			simpleModelRunConnection.connect();
			simpleModelRunConnection.executeQuery(getFeedFetchSimpleModelRunQuery);

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
}