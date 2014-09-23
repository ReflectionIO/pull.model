//  
//  DataAccountService.java
//  reflection.io
//
//  Created by William Shakour on December 26, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.dataaccount;

import static com.spacehopperstudios.utility.StringUtils.addslashes;
import static com.spacehopperstudios.utility.StringUtils.stripslashes;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.SortDirectionType;
import io.reflection.app.datatypes.shared.DataAccount;
import io.reflection.app.datatypes.shared.DataSource;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;
import io.reflection.app.shared.util.DataTypeHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

final class DataAccountService implements IDataAccountService {

	private static final Logger LOG = Logger.getLogger(DataAccountService.class.getName());

	private static final String KEY_PART_1 = "F54E1A22";
	private static final String KEY_PART_2 = "395D";
	private static final String KEY_PART_3 = "42B8";
	private static final String KEY_PART_4 = "9002";
	private static final String KEY_PART_5 = "61E14A750D98";

	public String getName() {
		return ServiceType.ServiceTypeDataAccount.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getDataAccount(java.lang.Long)
	 */
	@Override
	public DataAccount getDataAccount(Long id) throws DataAccessException {
		return getDataAccount(id, Boolean.FALSE);
	}

	/**
	 * 
	 * @param id
	 * @param deleted
	 *            If true, retrieve deleted linked accounts as well
	 * @return
	 * @throws DataAccessException
	 */
	@Override
	public DataAccount getDataAccount(Long id, Boolean deleted) throws DataAccessException {
		DataAccount dataAccount = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection dataAccountConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		String getDataAccountQuery = String.format(
				"SELECT *, convert(aes_decrypt(`password`,UNHEX('%s')), CHAR(1000)) AS `clearpassword` FROM `dataaccount` WHERE `id`=%d %s LIMIT 1", key(),
				id.longValue(), deleted ? "" : "AND `deleted`='n'");

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(getDataAccountQuery);

			if (dataAccountConnection.fetchNextRow()) {
				dataAccount = toDataAccount(dataAccountConnection);
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}
		return dataAccount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getDataAccount(java.lang.String, java.lang.Long)
	 */
	@Override
	public DataAccount getDataAccount(String username, Long sourceid) throws DataAccessException {
		return getDataAccount(username, sourceid, Boolean.FALSE);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getDataAccount(java.lang.String, java.lang.Long, java.lang.Boolean)
	 */
	@Override
	public DataAccount getDataAccount(String username, Long sourceid, Boolean deleted) throws DataAccessException {
		DataAccount dataAccount = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection dataAccountConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		String getDataAccountQuery = String
				.format("SELECT *, convert(aes_decrypt(`password`,UNHEX('%s')), CHAR(1000)) AS `clearpassword` FROM `dataaccount` WHERE `username`='%s' AND `sourceid`=%d %s LIMIT 1",
						key(), username, sourceid, deleted ? "" : "AND `deleted`='n'");

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(getDataAccountQuery);

			if (dataAccountConnection.fetchNextRow()) {
				dataAccount = toDataAccount(dataAccountConnection);
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}
		return dataAccount;
	}

	/**
	 * To dataAccount
	 * 
	 * @param connection
	 * @return
	 */
	private DataAccount toDataAccount(Connection connection) throws DataAccessException {
		DataAccount dataAccount = new DataAccount();
		dataAccount.id = connection.getCurrentRowLong("id");
		dataAccount.created = connection.getCurrentRowDateTime("created");
		dataAccount.deleted = connection.getCurrentRowString("deleted");
		dataAccount.active = connection.getCurrentRowString("active");
		dataAccount.source = new DataSource();
		dataAccount.source.id = connection.getCurrentRowLong("sourceid");

		dataAccount.username = stripslashes(connection.getCurrentRowString("username"));
		dataAccount.password = stripslashes(connection.getCurrentRowString("clearpassword")); // column name is password but all select queries should return
																								// decrypted password as clearpassword
		dataAccount.properties = stripslashes(connection.getCurrentRowString("properties"));

		return dataAccount;
	}

	private String key() {
		return KEY_PART_1 + KEY_PART_2 + KEY_PART_3 + KEY_PART_4 + KEY_PART_5;
	}

	@Override
	public DataAccount addDataAccount(DataAccount dataAccount) throws DataAccessException {

		DataAccount addedDataAccount = null;

		final String addDataAccountQuery = String.format(
				"INSERT INTO `dataaccount` (`sourceid`,`username`,`password`,`properties`) VALUES (%d,'%s',AES_ENCRYPT('%s',UNHEX('%s')),'%s')",
				dataAccount.source.id, addslashes(dataAccount.username), addslashes(dataAccount.password), key(), addslashes(dataAccount.properties));

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(addDataAccountQuery);

			if (dataAccountConnection.getAffectedRowCount() > 0) {
				addedDataAccount = getDataAccount(Long.valueOf(dataAccountConnection.getInsertedId()));

				if (addedDataAccount == null) {
					addedDataAccount = dataAccount;
					addedDataAccount.id = Long.valueOf(dataAccountConnection.getInsertedId());
				}
			}
		} catch (DataAccessException ex) {
			if (ex.getCause() instanceof MySQLIntegrityConstraintViolationException) { // Data account already exists
				// Restore deactivated Data Account
				Long restoredId = getDataAccount(dataAccount.username, dataAccount.source.id).id;
				dataAccount.id = restoredId;
				dataAccount.active = DataTypeHelper.ACTIVE_VALUE;
				addedDataAccount = updateDataAccount(dataAccount);
			} else {
				throw ex;
			}
		}

		finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}
		
		return addedDataAccount;
	}

	@Override
	public DataAccount updateDataAccount(DataAccount dataAccount) throws DataAccessException {

		DataAccount updatedDataAccount = null;

		final String updateDataAccountQuery = String
				.format("UPDATE `dataaccount` SET `username`='%s', `password`=AES_ENCRYPT('%s',UNHEX('%s')), `properties`='%s', `active`='%s' WHERE `id`=%d AND `deleted`='n'",
						addslashes(dataAccount.username), addslashes(dataAccount.password), key(), addslashes(dataAccount.properties),
						addslashes(dataAccount.active), dataAccount.id.longValue());

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(updateDataAccountQuery);

			if (dataAccountConnection.getAffectedRowCount() > 0) {
				updatedDataAccount = getDataAccount(dataAccount.id);
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}

		return updatedDataAccount;
	}

	@Override
	public DataAccount restoreDataAccount(DataAccount dataAccount) throws DataAccessException {

		DataAccount restoredDataAccount = null;

		final String restoreDataAccountQuery = String.format(
				"UPDATE `dataaccount` SET `created`=NOW(), `password`=AES_ENCRYPT('%s',UNHEX('%s')), `properties`='%s', `deleted`='n' WHERE `username`='%s'",
				addslashes(dataAccount.password), key(), addslashes(dataAccount.properties), addslashes(dataAccount.username));

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(restoreDataAccountQuery);

			if (dataAccountConnection.getAffectedRowCount() > 0) {
				dataAccount = getDataAccount(dataAccount.username, dataAccount.source.id);
				restoredDataAccount = getDataAccount(dataAccount.id);
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}
		
		return restoredDataAccount;
	}

	@Override
	public void deleteDataAccount(DataAccount dataAccount) throws DataAccessException {
		String deleteDataAccountQuery = String.format("UPDATE `dataaccount` SET `deleted`='y' WHERE `id`=%d AND `deleted`='n'", dataAccount.id.longValue());

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());
		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(deleteDataAccountQuery);

			if (dataAccountConnection.getAffectedRowCount() > 0) {
				if (LOG.isLoggable(Level.INFO)) {
					LOG.info(String.format("Account with id [%d] was marked as deleted", dataAccount.id.longValue()));
				}
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getDataAccounts(io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<DataAccount> getDataAccounts(Pager pager) throws DataAccessException {

		List<DataAccount> dataAccounts = new ArrayList<DataAccount>();

		String getDataAccountsQuery = String
				.format("SELECT *, convert(aes_decrypt(`password`,UNHEX('%s')), CHAR(1000)) AS `clearpassword` FROM `dataaccount` WHERE `deleted`='n' ORDER BY `%s` %s LIMIT %d,%d",
						key(), pager.sortBy == null ? "id" : pager.sortBy,
						pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC" : "DESC", pager.start, pager.count);

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(getDataAccountsQuery);

			while (dataAccountConnection.fetchNextRow()) {
				DataAccount dataAccount = toDataAccount(dataAccountConnection);

				if (dataAccount != null) {
					dataAccounts.add(dataAccount);
				}
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}

		return dataAccounts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getDataAccountsCount()
	 */
	@Override
	public Long getDataAccountsCount() throws DataAccessException {
		Long dataAccountsCount = Long.valueOf(0);

		String getDataAccountsCountQuery = String.format("SELECT count(1) AS `count` FROM `dataaccount` WHERE `deleted`='n' LIMIT 1");

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(getDataAccountsCountQuery);

			if (dataAccountConnection.fetchNextRow()) {
				dataAccountsCount = dataAccountConnection.getCurrentRowLong("count");
			}
		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}

		return dataAccountsCount;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#addDataAccount(io.reflection.app.shared.datatypes.DataSource, java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public DataAccount addDataAccount(DataSource dataSource, String username, String password, String properties) throws DataAccessException {
		DataAccount dataAccount = new DataAccount();

		dataAccount.source = dataSource;
		dataAccount.username = username;
		dataAccount.password = password;
		dataAccount.properties = properties;

		return addDataAccount(dataAccount);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#getIdsDataAccounts(java.util.Collection, io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<DataAccount> getIdsDataAccounts(Collection<Long> ids, Pager pager) throws DataAccessException {
		List<DataAccount> dataAccounts = new ArrayList<DataAccount>();

		StringBuffer joinedIds = new StringBuffer();

		for (Long id : ids) {
			if (joinedIds.length() != 0) {
				joinedIds.append(",");
			}

			joinedIds.append(id.toString());
		}

		String getIdsDataAccountsQuery = String
				.format("SELECT *, convert(aes_decrypt(`password`,UNHEX('%s')), CHAR(1000)) AS `clearpassword` FROM `dataaccount` WHERE `id` in (%s) AND `deleted`='n' ORDER BY `%s` %s",
						key(), joinedIds, pager.sortBy == null ? "id" : pager.sortBy,
						pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC" : "DESC");

		Connection dataAccountConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeDataAccount.toString());

		try {
			dataAccountConnection.connect();
			dataAccountConnection.executeQuery(getIdsDataAccountsQuery);

			while (dataAccountConnection.fetchNextRow()) {
				DataAccount dataAccount = toDataAccount(dataAccountConnection);

				if (dataAccount != null) {
					dataAccounts.add(dataAccount);
				}
			}

		} finally {
			if (dataAccountConnection != null) {
				dataAccountConnection.disconnect();
			}
		}

		return dataAccounts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#triggerDataAccountFetch(io.reflection.app.datatypes.shared.DataAccount)
	 */
	@Override
	public void triggerDataAccountFetch(DataAccount dataAccount) throws DataAccessException {}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.dataaccount.IDataAccountService#triggerSingleDateDataAccountFetch(io.reflection.app.datatypes.shared.DataAccount,
	 * java.util.Date)
	 */
	@Override
	public void triggerSingleDateDataAccountFetch(DataAccount dataAccount, Date date) throws DataAccessException {}

}