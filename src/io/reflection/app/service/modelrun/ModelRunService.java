//  
//  ModelRunService.java
//  reflection.io
//
//  Created by William Shakour on October 23, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.modelrun;

import static com.spacehopperstudios.utility.StringUtils.addslashes;
import static com.spacehopperstudios.utility.StringUtils.stripslashes;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.FormType;
import io.reflection.app.datatypes.shared.ModelRun;
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

final class ModelRunService implements IModelRunService {

	public String getName() {
		return ServiceType.ServiceTypeModelRun.toString();
	}

	@Override
	public ModelRun getModelRun(Long id) throws DataAccessException {
		ModelRun modelRun = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection modelRunConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeModelRun.toString());

		final String getModelRunQuery = String.format("SELECT * FROM `modelrun` WHERE `deleted`='n' AND `id`='%d' LIMIT 1", id.longValue());

		try {
			modelRunConnection.connect();
			modelRunConnection.executeQuery(getModelRunQuery);

			if (modelRunConnection.fetchNextRow()) {
				modelRun = toModelRun(modelRunConnection);
			}
		} finally {
			if (modelRunConnection != null) {
				modelRunConnection.disconnect();
			}
		}
		return modelRun;
	}

	/**
	 * To modelRun
	 * 
	 * @param connection
	 * @return
	 * @throws DataAccessException
	 */
	private ModelRun toModelRun(Connection connection) throws DataAccessException {
		ModelRun modelRun = new ModelRun();

		modelRun.id = connection.getCurrentRowLong("id");
		modelRun.created = connection.getCurrentRowDateTime("created");
		modelRun.deleted = connection.getCurrentRowString("deleted");

		modelRun.country = stripslashes(connection.getCurrentRowString("country"));
		modelRun.store = stripslashes(connection.getCurrentRowString("store"));

		modelRun.code = connection.getCurrentRowLong("code2");
		modelRun.form = FormType.fromString(connection.getCurrentRowString("form"));
		modelRun.grossingA = connection.getCurrentRowDouble("grossinga");
		modelRun.paidA = connection.getCurrentRowDouble("paida");
		modelRun.bRatio = connection.getCurrentRowDouble("bratio");
		modelRun.totalDownloads = connection.getCurrentRowDouble("totaldownloads");
		modelRun.paidB = connection.getCurrentRowDouble("paidb");
		modelRun.grossingB = connection.getCurrentRowDouble("grossingb");
		modelRun.paidAIap = connection.getCurrentRowDouble("paidaiap");
		modelRun.grossingAIap = connection.getCurrentRowDouble("grossingaiap");
		modelRun.freeA = connection.getCurrentRowDouble("freea");
		modelRun.theta = connection.getCurrentRowDouble("theta");
		modelRun.freeB = connection.getCurrentRowDouble("freeb");

		return modelRun;
	}

	@Override
	public ModelRun addModelRun(ModelRun modelRun) throws DataAccessException {
		ModelRun addedModelRun = null;

		final String addModelRunQuery = String
				.format("INSERT INTO `modelrun` (`country`,`store`,`code2`,`form`,`grossinga`,`paida`,`bratio`,`totaldownloads`,`paidb`,`grossingb`,`paidaiap`,`grossingaiap`,`freea`,`theta`,`freeb`) VALUES ('%s','%s',%d,'%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f);",
						addslashes(modelRun.country), addslashes(modelRun.store), modelRun.code.longValue(), modelRun.form.toString(),
						modelRun.grossingA.doubleValue(), modelRun.paidA.doubleValue(), modelRun.bRatio.doubleValue(), modelRun.totalDownloads.doubleValue(),
						modelRun.paidB.doubleValue(), modelRun.grossingB.doubleValue(), modelRun.paidAIap.doubleValue(), modelRun.grossingAIap.doubleValue(),
						modelRun.freeA.doubleValue(), modelRun.theta.doubleValue(), modelRun.freeB.doubleValue());

		Connection modelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeModelRun.toString());

		try {
			modelRunConnection.connect();
			modelRunConnection.executeQuery(addModelRunQuery);

			if (modelRunConnection.getAffectedRowCount() > 0) {
				addedModelRun = getModelRun(Long.valueOf(modelRunConnection.getInsertedId()));

				if (addedModelRun == null) {
					addedModelRun = modelRun;
					addedModelRun.id = Long.valueOf(modelRunConnection.getInsertedId());
				}
			}
		} finally {
			if (modelRunConnection != null) {
				modelRunConnection.disconnect();
			}
		}

		return addedModelRun;
	}

	@Override
	public ModelRun updateModelRun(ModelRun modelRun) throws DataAccessException {
		ModelRun updatedModelRun = null;

		final String updateModelRunQuery = String
				.format("UPDATE `modelrun` SET `country`='%s',`store`='%s',`code2`=%d,`form`='%s',`grossinga`=%f,`paida`=%f,`bratio`=%f,`totaldownloads`=%f,`paidb`=%f,`grossingb`=%f,`paidaiap`=%f,`grossingaiap`=%f,`freea`=%f,`theta`=%f,`freeb`=%f WHERE `id`=%d AND `deleted`='n';",
						addslashes(modelRun.country), addslashes(modelRun.store), modelRun.code.longValue(), modelRun.form.toString(),
						modelRun.grossingA.doubleValue(), modelRun.paidA.doubleValue(), modelRun.bRatio.doubleValue(), modelRun.totalDownloads.doubleValue(),
						modelRun.paidB.doubleValue(), modelRun.grossingB.doubleValue(), modelRun.paidAIap.doubleValue(), modelRun.grossingAIap.doubleValue(),
						modelRun.freeA.doubleValue(), modelRun.theta.doubleValue(), modelRun.freeB.doubleValue(), modelRun.id.longValue());

		Connection modelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeModelRun.toString());

		try {
			modelRunConnection.connect();
			modelRunConnection.executeQuery(updateModelRunQuery);

			if (modelRunConnection.getAffectedRowCount() > 0) {
				updatedModelRun = getModelRun(modelRun.id);
			} else {
				updatedModelRun = modelRun;
			}

		} finally {
			if (modelRunConnection != null) {
				modelRunConnection.disconnect();
			}
		}

		return updatedModelRun;
	}

	@Override
	public void deleteModelRun(ModelRun modelRun) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.modelrun.IModelRunService#getGatherCodeModelRun(java.lang.String, java.lang.String,
	 * io.reflection.app.shared.datatypes.FormType, java.lang.Long)
	 */
	@Override
	public ModelRun getGatherCodeModelRun(Country country, Store store, FormType form, Long code) throws DataAccessException {
		ModelRun modelRun = null;

		final String getGatherCodeModelRunQuery = String.format(
				"SELECT * FROM `modelrun` WHERE `store`='%s' AND `country`='%s' AND `form`='%s' AND `code2`=%d", addslashes(store.a3Code),
				addslashes(country.a2Code), form.toString(), code.longValue());

		Connection modelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeModelRun.toString());

		try {
			modelRunConnection.connect();
			modelRunConnection.executeQuery(getGatherCodeModelRunQuery);

			if (modelRunConnection.fetchNextRow()) {
				modelRun = toModelRun(modelRunConnection);
			}
		} finally {
			if (modelRunConnection != null) {
				modelRunConnection.disconnect();
			}
		}

		return modelRun;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.modelrun.IModelRunService#getModelRun(io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Store, io.reflection.app.datatypes.shared.FormType, java.util.Date, java.util.Date)
	 */
	@Override
	public ModelRun getModelRun(Country country, Store store, FormType form, Date start, Date end) throws DataAccessException {
		ModelRun modelRun = null;

		Long code = FeedFetchServiceProvider.provide().getGatherCode(country, store, start, end);

		String getModelRunQuery = String
				.format("SELECT * FROM `modelrun` WHERE CAST(`country` AS BINARY)=CAST('%s' AS BINARY) AND CAST(`store` AS BINARY)=CAST('%s' AS BINARY) AND `code2`=%d `deleted`='n' ORDER BY `id` DESC LIMIT 1",
						addslashes(country.a2Code), addslashes(store.a3Code), code.longValue());

		Connection modelRunConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeModelRun.toString());

		try {
			modelRunConnection.connect();
			modelRunConnection.executeQuery(getModelRunQuery);

			if (modelRunConnection.fetchNextRow()) {
				modelRun = toModelRun(modelRunConnection);
			}
		} finally {
			if (modelRunConnection != null) {
				modelRunConnection.disconnect();
			}
		}

		return modelRun;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.modelrun.IModelRunService#getDateModelRunBatch(io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Store, io.reflection.app.datatypes.shared.FormType, java.util.Collection)
	 */
	@Override
	public List<ModelRun> getDateModelRunBatch(Country country, Store store, FormType form, Collection<Date> dates) throws DataAccessException {
		List<ModelRun> modelRuns = new ArrayList<ModelRun>();
		
		
		
		return modelRuns;
	}

}