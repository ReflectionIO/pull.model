//  
//  SaleService.java
//  reflection.io
//
//  Created by William Shakour on December 30, 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyrights © 2013 reflection.io. All rights reserved.
//
package io.reflection.app.service.sale;

import static com.spacehopperstudios.utility.StringUtils.addslashes;
import static com.spacehopperstudios.utility.StringUtils.stripslashes;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.SortDirectionType;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Country;
import io.reflection.app.datatypes.shared.DataAccount;
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.datatypes.shared.Sale;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;
import io.reflection.app.service.dataaccount.DataAccountServiceProvider;
import io.reflection.app.service.item.ItemServiceProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spacehopperstudios.utility.StringUtils;

final class SaleService implements ISaleService {
	public String getName() {
		return ServiceType.ServiceTypeSale.toString();
	}

	@Override
	public Sale getSale(Long id) throws DataAccessException {
		Sale sale = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection saleConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		String getSaleQuery = String.format("SELECT * FROM `sale` WHERE `deleted`='n' AND `id`='%d' LIMIT 1", id.longValue());
		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSaleQuery);

			if (saleConnection.fetchNextRow()) {
				sale = toSale(saleConnection);
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}
		return sale;
	}

	/**
	 * To sale
	 * 
	 * @param connection
	 * @return
	 * @throws DataAccessException
	 */
	private Sale toSale(Connection connection) throws DataAccessException {
		Sale sale = new Sale();
		sale.id = connection.getCurrentRowLong("id");
		sale.created = connection.getCurrentRowDateTime("created");
		sale.deleted = connection.getCurrentRowString("deleted");

		sale.account = new DataAccount();
		sale.account.id = connection.getCurrentRowLong("dataaccountid");

		sale.item = new Item();
		sale.item.internalId = connection.getCurrentRowString("itemid");

		sale.country = stripslashes(connection.getCurrentRowString("country"));
		sale.sku = stripslashes(connection.getCurrentRowString("sku"));
		sale.developer = stripslashes(connection.getCurrentRowString("developer"));
		sale.title = stripslashes(connection.getCurrentRowString("title"));
		sale.version = stripslashes(connection.getCurrentRowString("version"));
		sale.typeIdentifier = stripslashes(connection.getCurrentRowString("typeidentifier"));
		sale.units = connection.getCurrentRowInteger("units");
		sale.proceeds = Float.valueOf(connection.getCurrentRowInteger("proceeds").floatValue() / 100.0f);
		sale.currency = stripslashes(connection.getCurrentRowString("currency"));
		sale.begin = connection.getCurrentRowDateTime("begin");
		sale.end = connection.getCurrentRowDateTime("end");
		sale.customerCurrency = stripslashes(connection.getCurrentRowString("customercurrency"));
		sale.customerPrice = Float.valueOf(connection.getCurrentRowInteger("customerprice").floatValue() / 100.0f);
		sale.promoCode = stripslashes(connection.getCurrentRowString("promocode"));
		sale.parentIdentifier = stripslashes(connection.getCurrentRowString("parentidentifier"));
		sale.subscription = stripslashes(connection.getCurrentRowString("subscription"));
		sale.period = stripslashes(connection.getCurrentRowString("period"));
		sale.category = stripslashes(connection.getCurrentRowString("category"));

		return sale;
	}

	@Override
	public Sale addSale(Sale sale) throws DataAccessException {
		Sale addedSale = null;

		// TODO: sort out nullable values

		final String addSaleQuery = String
				.format("INSERT INTO `sale` (`dataaccountid`,`itemid`,`country`,`sku`,`developer`,`title`,`version`,`typeidentifier`,`units`,`proceeds`,`currency`,`begin`,`end`,`customercurrency`,`customerprice`,`promocode`,`parentidentifier`,`subscription`,`period`,`category`) VALUES (%d,%d,'%s','%s','%s','%s','%s','%s',%d,%d,'%s',FROM_UNIXTIME(%d),FROM_UNIXTIME(%d),'%s',%d,'%s','%s','%s','%s','%s')",
						sale.account.id.longValue(), sale.item.id.longValue(), addslashes(sale.country), addslashes(sale.sku), addslashes(sale.developer),
						addslashes(sale.title), addslashes(sale.version), addslashes(sale.typeIdentifier), sale.units.intValue(),
						(int) (sale.proceeds.floatValue() * 100.0f), addslashes(sale.currency), sale.begin.getTime() / 1000, sale.end.getTime() / 1000,
						addslashes(sale.customerCurrency), (int) (sale.customerPrice.floatValue() * 100.0f), addslashes(sale.promoCode),
						addslashes(sale.parentIdentifier), addslashes(sale.subscription), addslashes(sale.period), addslashes(sale.category));

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(addSaleQuery);

			if (saleConnection.getAffectedRowCount() > 0) {
				addedSale = getSale(Long.valueOf(saleConnection.getInsertedId()));

				if (addedSale == null) {
					addedSale = sale;
					addedSale.id = Long.valueOf(saleConnection.getInsertedId());
				}
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return addedSale;
	}

	@Override
	public Sale updateSale(Sale sale) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteSale(Sale sale) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getDataAccountItems(io.reflection.app.shared.datatypes.DataAccount,
	 * io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Item> getDataAccountItems(DataAccount dataAccount, List<String> typeIdentifiers, Pager pager) throws DataAccessException {
		List<String> itemIds = new ArrayList<String>();
		List<String> itemIdsTop400 = new ArrayList<String>();
		List<Item> items = new ArrayList<Item>();

		String typeId = "";
		if (typeIdentifiers != null && typeIdentifiers.size() > 0) {
			typeId = "AND `typeidentifier` IN ('" + StringUtils.join(typeIdentifiers, "','") + "')";
		}

		String getSaleQuery = String.format("SELECT DISTINCT `itemid` FROM `sale` WHERE `dataaccountid`=%d %s AND `deleted`='n' ORDER BY `%s` %s LIMIT %d, %d",
				dataAccount.id.longValue(), typeId, pager.sortBy == null ? "id" : pager.sortBy,
				pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC" : "DESC", pager.start == null ? Pager.DEFAULT_START.longValue()
						: pager.start.longValue(), pager.count == null ? Pager.DEFAULT_COUNT.longValue() : pager.count.longValue());

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection saleConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSaleQuery);

			while (saleConnection.fetchNextRow()) {
				itemIds.add(saleConnection.getCurrentRowString("itemid"));
			}

			items.addAll(ItemServiceProvider.provide().getInternalIdItemBatch(itemIds)); // return only Items in the top 400

			for (Item i : items) {
				itemIdsTop400.add(i.internalId);
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		itemIds.removeAll(itemIdsTop400); // get IDs of items out of the top 400

		if (itemIds.size() > 0) {
			items.addAll(generateDummyItems(itemIds, typeIdentifiers)); // generate dummy items from sale table for items out of the top 400
		}

		return items;
	}

	/**
	 * Generate dummy items from sale table for items out of the top 400
	 * 
	 * @param itemId
	 * @return
	 * @throws DataAccessException
	 */
	private List<Item> generateDummyItems(Collection<String> itemId, List<String> typeIdentifiers) throws DataAccessException {
		List<Item> items = new ArrayList<Item>();

		String typeId = "";
		if (typeIdentifiers != null && typeIdentifiers.size() > 0) {
			typeId = "AND `typeidentifier` IN ('" + StringUtils.join(typeIdentifiers, "','") + "')";
		}

		String typesQueryPart = null;
		if (itemId.size() == 1) {
			typesQueryPart = String.format("CAST(`itemid` AS BINARY)=CAST('%s' AS BINARY)", itemId.iterator().next());
		} else {
			typesQueryPart = "CAST(`itemid` AS BINARY) IN (CAST('" + StringUtils.join(itemId, "' AS BINARY),CAST('") + "' AS BINARY))";
		}

		String getSaleItemQuery = String.format(
				"SELECT DISTINCT `title`,`developer`,`itemid`,`sku` FROM `sale` WHERE %s AND `deleted`='n' %s GROUP BY `itemid`", typesQueryPart, typeId);
		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection saleConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSaleItemQuery);

			Map<String, Item> skuItemLookup = new HashMap<String, Item>();
			while (saleConnection.fetchNextRow()) {
				Item mockItem = toMockItem(saleConnection);
				skuItemLookup.put(saleConnection.getCurrentRowString("sku"), mockItem);
			}

			// Add IAP property if has IA1 or IA9 sales
			String parentIdentifiers = "";
			if (skuItemLookup.size() > 0) {
				parentIdentifiers = "AND `parentidentifier` IN ('" + StringUtils.join(skuItemLookup.keySet(), "','") + "')";
			}
			String getIAPQuery = String.format("SELECT DISTINCT parentidentifier FROM `sale` WHERE `typeidentifier` IN ('IA1','IA9') %s", parentIdentifiers);

			saleConnection.executeQuery(getIAPQuery);
			while (saleConnection.fetchNextRow()) {
				skuItemLookup.get(saleConnection.getCurrentRowString("parentidentifier")).properties = "{\"usesIap\":true}";
			}

			items.addAll(skuItemLookup.values());

		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return items;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getDataAccountItemsCount()
	 */
	@Override
	public Long getDataAccountItemsCount(DataAccount dataAccount, List<String> typeIdentifiers) throws DataAccessException {

		Long dataCount = Long.valueOf(0);
		String typeId = "";
		if (typeIdentifiers != null && typeIdentifiers.size() > 0) {
			typeId = "AND `typeidentifier` IN ('" + StringUtils.join(typeIdentifiers, "','") + "')";
		}
		String getDataAccountsCountQuery = String.format(
				"SELECT COUNT(DISTINCT `itemid`) AS `datacount` FROM `sale` WHERE `deleted`='n' AND `dataaccountid`=%d %s", dataAccount.id.longValue(), typeId);

		Connection dataConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());
		try {
			dataConnection.connect();
			dataConnection.executeQuery(getDataAccountsCountQuery);

			if (dataConnection.fetchNextRow()) {
				dataCount = dataConnection.getCurrentRowLong("datacount");
			}
		} finally {
			if (dataConnection != null) {
				dataConnection.disconnect();
			}
		}

		return dataCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#addSalesBatch(java.util.Collection)
	 */
	@Override
	public Long addSalesBatch(Collection<Sale> sales) throws DataAccessException {
		Long addedSalesBatchCount = Long.valueOf(0);

		// TODO: sort out nullable values

		StringBuffer addSalesBatchQuery = new StringBuffer();

		addSalesBatchQuery
				.append("INSERT INTO `sale` (`dataaccountid`,`itemid`,`country`,`sku`,`developer`,`title`,`version`,`typeidentifier`,`units`,`proceeds`,`currency`,`begin`,`end`,`customercurrency`,`customerprice`,`promocode`,`parentidentifier`,`subscription`,`period`,`category`) VALUES");

		for (Sale sale : sales) {
			if (addSalesBatchQuery.charAt(addSalesBatchQuery.length() - 1) != 'S') {
				addSalesBatchQuery.append(",");
			}

			addSalesBatchQuery.append(String.format(
					"(%d,%s,'%s','%s','%s','%s','%s','%s',%d,%d,'%s',FROM_UNIXTIME(%d),FROM_UNIXTIME(%d),'%s',%d,'%s','%s','%s','%s','%s')",
					sale.account.id.longValue(), sale.item.internalId == null ? "NULL" : "'" + sale.item.internalId + "'", addslashes(sale.country),
					addslashes(sale.sku), addslashes(sale.developer), addslashes(sale.title), addslashes(sale.version), addslashes(sale.typeIdentifier),
					sale.units.intValue(), (int) (sale.proceeds.floatValue() * 100.0f), addslashes(sale.currency), sale.begin.getTime() / 1000,
					sale.end.getTime() / 1000, addslashes(sale.customerCurrency), (int) (sale.customerPrice.floatValue() * 100.0f), addslashes(sale.promoCode),
					addslashes(sale.parentIdentifier), addslashes(sale.subscription), addslashes(sale.period), addslashes(sale.category)));
		}

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(addSalesBatchQuery.toString());

			addedSalesBatchCount = Long.valueOf(saleConnection.getAffectedRowCount());
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return addedSalesBatchCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getSales(io.reflection.app.datatypes.shared.Country, io.reflection.app.datatypes.shared.Category,
	 * io.reflection.app.datatypes.shared.DataAccount, java.util.Date, java.util.Date, io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Sale> getSales(Country country, Category category, DataAccount linkedAccount, Date start, Date end, Pager pager) throws DataAccessException {
		List<Sale> sales = new ArrayList<Sale>();

		// FIXME: for now we use the all category for the iOS store... we should get the category passed in, or attempt to detect it based on the linked account
		// (category relates to store by a3code)
		// we are using end for date but we could equally use begin
		String getSalesQuery = String
				.format("SELECT * FROM `sale` WHERE `country`='%s' AND (%d=%d OR `category`='%s') AND `dataaccountid`=%d AND (`end` BETWEEN FROM_UNIXTIME(%d) AND FROM_UNIXTIME(%d)) AND `deleted`='n'",
						country.a2Code, 24, category.id.longValue(), category.name, linkedAccount.id.longValue(), start.getTime() / 1000, end.getTime() / 1000);

		if (pager != null) {
			String sortByQuery = "id";

			// if (pager.sortBy != null && ("code".equals(pager.sortBy) || "name".equals(pager.sortBy))) {
			// sortByQuery = pager.sortBy;
			// }

			String sortDirectionQuery = "DESC";

			if (pager.sortDirection != null) {
				switch (pager.sortDirection) {
				case SortDirectionTypeAscending:
					sortDirectionQuery = "ASC";
					break;
				default:
					break;
				}
			}

			getSalesQuery += String.format(" ORDER BY `%s` %s", sortByQuery, sortDirectionQuery);
		}

		if (pager.start != null && pager.count != null) {
			getSalesQuery += String.format(" LIMIT %d, %d", pager.start.longValue(), pager.count.longValue());
		} else if (pager.count != null) {
			getSalesQuery += String.format(" LIMIT %d", pager.count.longValue());
		}

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSalesQuery);

			while (saleConnection.fetchNextRow()) {
				Sale sale = toSale(saleConnection);

				if (sale != null) {
					sales.add(sale);
				}
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return sales;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getSalesCount(io.reflection.app.datatypes.shared.Country, io.reflection.app.datatypes.shared.Category,
	 * io.reflection.app.datatypes.shared.DataAccount, java.util.Date, java.util.Date)
	 */
	@Override
	public Long getSalesCount(Country country, Category category, DataAccount linkedAccount, Date start, Date end) throws DataAccessException {
		Long salesCount = Long.valueOf(0);

		String getSalesQuery = String
				.format("SELECT count(1) AS `salescount` FROM `sale` WHERE `country`='%s' AND (%d=%d OR `category`='%s') AND `dataaccountid`=%d (`end` BETWEEN FROM_UNIXTIME(%d) AND FROM_UNIXTIME(%d)) AND `deleted`='n'",
						country.a2Code, 24, category.id.longValue(), category.name, linkedAccount.id.longValue(), start.getTime() / 1000, end.getTime() / 1000);

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSalesQuery);

			if (saleConnection.fetchNextRow()) {
				salesCount = saleConnection.getCurrentRowLong("salescount");
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return salesCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getItemSales(io.reflection.app.datatypes.shared.Item, io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Category, io.reflection.app.datatypes.shared.DataAccount, java.util.Date, java.util.Date,
	 * io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Sale> getItemSales(Item item, Country country, Category category, DataAccount linkedAccount, Date start, Date end, Pager pager)
			throws DataAccessException {
		List<Sale> sales = new ArrayList<Sale>();

		// FIXME: for now we use the all category for the iOS store... we should get the category passed in, or attempt
		// to detect it based on the linked account
		// (category relates to store by a3code)
		// we are using end for date but we could equally use begin
		String getSalesQuery = String
				.format("SELECT * FROM `sale` WHERE `country`='%s' AND (%d=%d OR `category`='%s') AND `dataaccountid`=%d AND (`end` BETWEEN FROM_UNIXTIME(%d) AND FROM_UNIXTIME(%d)) AND `itemid`='%s' AND `deleted`='n'",
						country.a2Code, 24, category.id.longValue(), category.name, linkedAccount.id.longValue(), start.getTime() / 1000, end.getTime() / 1000,
						item.internalId);

		if (pager != null) {
			String sortByQuery = "id";

			// if (pager.sortBy != null && ("code".equals(pager.sortBy) || "name".equals(pager.sortBy))) {
			// sortByQuery = pager.sortBy;
			// }

			String sortDirectionQuery = "DESC";

			if (pager.sortDirection != null) {
				switch (pager.sortDirection) {
				case SortDirectionTypeAscending:
					sortDirectionQuery = "ASC";
					break;
				default:
					break;
				}
			}

			getSalesQuery += String.format(" ORDER BY `%s` %s", sortByQuery, sortDirectionQuery);
		}

		if (pager.start != null && pager.count != null) {
			getSalesQuery += String.format(" LIMIT %d, %d", pager.start.longValue(), pager.count.longValue());
		} else if (pager.count != null) {
			getSalesQuery += String.format(" LIMIT %d", pager.count.longValue());
		}

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSalesQuery);

			while (saleConnection.fetchNextRow()) {
				Sale sale = toSale(saleConnection);

				if (sale != null) {
					sales.add(sale);
				}
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return sales;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getItemSalesCount(io.reflection.app.datatypes.shared.Item, io.reflection.app.datatypes.shared.Country,
	 * io.reflection.app.datatypes.shared.Category, io.reflection.app.datatypes.shared.DataAccount, java.util.Date, java.util.Date)
	 */
	@Override
	public Long getItemSalesCount(Item item, Country country, Category category, DataAccount linkedAccount, Date start, Date end) throws DataAccessException {
		Long salesCount = Long.valueOf(0);

		String getSalesQuery = String
				.format("SELECT count(1) AS `salescount` FROM `sale` WHERE `country`='%s' AND (%d=%d OR `category`='%s') AND `dataaccountid`=%d (`end` BETWEEN FROM_UNIXTIME(%d) AND FROM_UNIXTIME(%d)) AND `itemid`='%s' AND `deleted`='n'",
						country.a2Code, 24, category.id.longValue(), category.name, linkedAccount.id.longValue(), start.getTime() / 1000, end.getTime() / 1000,
						item.internalId);

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getSalesQuery);

			if (saleConnection.fetchNextRow()) {
				salesCount = saleConnection.getCurrentRowLong("salescount");
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return salesCount;
	}

	private Item toMockItem(Connection connection) throws DataAccessException {
		Item mockItem = new Item();

		mockItem.creatorName = connection.getCurrentRowString("developer");
		mockItem.name = connection.getCurrentRowString("title");
		mockItem.internalId = connection.getCurrentRowString("itemid");

		return mockItem;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getItem(java.lang.String)
	 */
	@Override
	public Item getItem(String itemId) throws DataAccessException {
		Item item = null;

		String getItemQuery = String.format("SELECT `developer`, `title`, `itemid` FROM `sale` WHERE `itemid`='%s' AND `deleted`='n' LIMIT 1", itemId);

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getItemQuery);

			if (saleConnection.fetchNextRow()) {
				item = toMockItem(saleConnection);
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}
		return item;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.sale.ISaleService#getDataAccount(java.lang.String)
	 */
	@Override
	public DataAccount getDataAccount(String itemId) throws DataAccessException {
		DataAccount dataAccount = null;

		String getDataAccountIdQuery = String.format("SELECT `dataaccountid` FROM `sale` WHERE `itemid`='%s' AND `deleted`='n' LIMIT 1", itemId);

		Connection saleConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeSale.toString());

		try {
			saleConnection.connect();
			saleConnection.executeQuery(getDataAccountIdQuery);

			if (saleConnection.fetchNextRow()) {
				dataAccount = DataAccountServiceProvider.provide().getDataAccount(saleConnection.getCurrentRowLong("dataaccountid"));
			}
		} finally {
			if (saleConnection != null) {
				saleConnection.disconnect();
			}
		}

		return dataAccount;
	}

}