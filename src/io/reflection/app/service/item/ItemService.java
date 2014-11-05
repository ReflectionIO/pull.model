//  
//  ItemService.java
//  storedata
//
//  Created by William Shakour on 18 July 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//

package io.reflection.app.service.item;

import static com.spacehopperstudios.utility.StringUtils.addslashes;
import static com.spacehopperstudios.utility.StringUtils.stripslashes;
import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.SortDirectionType;
import io.reflection.app.datatypes.shared.Item;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spacehopperstudios.utility.StringUtils;

final class ItemService implements IItemService {

	public String getName() {
		return ServiceType.ServiceTypeItem.toString();
	}

	@Override
	public Item getItem(Long id) throws DataAccessException {
		Item item = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection itemConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getItemQuery = String.format("SELECT * FROM `item` WHERE `deleted`='n' AND `id`=%d LIMIT 1", id.longValue());
		try {
			itemConnection.connect();
			itemConnection.executeQuery(getItemQuery);

			if (itemConnection.fetchNextRow()) {
				item = toItem(itemConnection);
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return item;
	}

	/**
	 * To item
	 * 
	 * @param connection
	 * @return
	 * @throws DataAccessException
	 */
	private Item toItem(Connection connection) throws DataAccessException {
		Item item = new Item();

		item.id = connection.getCurrentRowLong("id");
		item.created = connection.getCurrentRowDateTime("created");
		item.deleted = connection.getCurrentRowString("deleted");

		item.added = connection.getCurrentRowDateTime("added");
		item.creatorName = stripslashes(connection.getCurrentRowString("creatorname"));
		item.country = stripslashes(connection.getCurrentRowString("country"));
		item.currency = stripslashes(connection.getCurrentRowString("currency"));
		item.externalId = stripslashes(connection.getCurrentRowString("externalid"));
		item.internalId = stripslashes(connection.getCurrentRowString("internalid"));
		item.name = stripslashes(connection.getCurrentRowString("name"));
		item.price = Float.valueOf(connection.getCurrentRowInteger("price").floatValue() / 100.0f);

		item.smallImage = stripslashes(connection.getCurrentRowString("smallimage"));
		item.mediumImage = stripslashes(connection.getCurrentRowString("mediumimage"));
		item.largeImage = stripslashes(connection.getCurrentRowString("largeimage"));

		item.properties = stripslashes(connection.getCurrentRowString("properties"));
		item.source = stripslashes(connection.getCurrentRowString("source"));
		item.type = stripslashes(connection.getCurrentRowString("type"));

		return item;
	}

	@Override
	public Item addItem(Item item) throws DataAccessException {
		Item addedItem = null;

		final String addItemQuery = String
				.format("INSERT INTO `item` (`externalid`,`internalid`,`name`,`creatorname`,`price`,`source`,`type`,`added`,`country`,`currency`,`smallimage`,`mediumimage`,`largeimage`,`properties`) VALUES ('%s','%s','%s','%s',%d,'%s','%s',FROM_UNIXTIME(%d),'%s','%s','%s','%s','%s','%s')",
						addslashes(item.externalId), addslashes(item.internalId), addslashes(item.name), addslashes(item.creatorName),
						(int) (item.price.floatValue() * 100.0f), addslashes(item.source), addslashes(item.type), item.added.getTime() / 1000,
						addslashes(item.country), addslashes(item.currency), addslashes(item.smallImage), addslashes(item.mediumImage),
						addslashes(item.largeImage), addslashes(item.properties));

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(addItemQuery);

			if (itemConnection.getAffectedRowCount() > 0) {
				addedItem = getItem(Long.valueOf(itemConnection.getInsertedId()));

				if (addedItem == null) {
					addedItem = item;
					addedItem.id = Long.valueOf(itemConnection.getInsertedId());
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return addedItem;
	}

	@Override
	public Item updateItem(Item item) throws DataAccessException {
		Item updatedItem = null;

		String updateItemQuery = String
				.format("UPDATE `item` SET `externalid`='%s',`internalid`='%s',`name`='%s',`creatorname`='%s',`price`=%d,`source`='%s',`type`='%s',`added`=FROM_UNIXTIME(%d),`country`='%s',`currency`='%s',`smallimage`='%s',`mediumimage`='%s',`largeimage`='%s',`properties`='%s' WHERE `id`=%d",
						addslashes(item.externalId), addslashes(item.internalId), addslashes(item.name), addslashes(item.creatorName),
						(int) (item.price.floatValue() * 100.0f), addslashes(item.source), addslashes(item.type), item.added.getTime() / 1000,
						addslashes(item.country), addslashes(item.currency), addslashes(item.smallImage), addslashes(item.mediumImage),
						addslashes(item.largeImage), addslashes(item.properties), item.id.longValue());

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeRank.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(updateItemQuery);

			if (itemConnection.getAffectedRowCount() > 0) {
				updatedItem = getItem(item.id);
			} else {
				updatedItem = item;
			}

		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return updatedItem;
	}

	@Override
	public void deleteItem(Item item) throws DataAccessException {
		String deleteItemQuery = String.format("UPDATE `item` SET `deleted`='y' WHERE `id`=%d", item.id.longValue());

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeRank.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(deleteItemQuery);
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getExternalIdItem(java.lang.String)
	 */
	@Override
	public Item getExternalIdItem(String externalId) throws DataAccessException {
		Item item = null;

		final String getExternalIdItemQuery = String.format("SELECT * FROM `item` WHERE `externalid`='%s' AND `deleted`='n' ORDER BY `id` DESC LIMIT 1",
				addslashes(externalId));
		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getExternalIdItemQuery);

			if (itemConnection.fetchNextRow()) {
				item = toItem(itemConnection);
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return item;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getInternalIdItem(java.lang.String)
	 */
	@Override
	public Item getInternalIdItem(String internalId) throws DataAccessException {
		Item item = null;
		final String getInternalIdItemQuery = String.format("SELECT * FROM `item` WHERE `internalid`='%s' AND `deleted`='n' ORDER BY `id` DESC LIMIT 1",
				addslashes(internalId));

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getInternalIdItemQuery);

			if (itemConnection.fetchNextRow()) {
				item = toItem(itemConnection);
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return item;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getExternalIdItemBatch(java.util.Collection)
	 */
	@Override
	public List<Item> getExternalIdItemBatch(Collection<String> itemIds) throws DataAccessException {
		List<Item> items = new ArrayList<Item>();

		String commaDelimitedItemIds = null;

		if (itemIds != null && itemIds.size() > 0) {
			List<String> keys = new ArrayList<String>();
			String memcacheKey = null;
			for (String externalId : itemIds) {
				memcacheKey = getName() + ".external." + externalId;
				keys.add(memcacheKey);
			}

			Item item = null;
			if (itemIds.size() > 0) {
				commaDelimitedItemIds = StringUtils.join(itemIds, "','");

				if (commaDelimitedItemIds != null && commaDelimitedItemIds.length() != 0) {
					String getExternalIdItemBatchQuery = String.format("SELECT * FROM `item` WHERE `externalid` IN ('%s') and `deleted`='n'",
							commaDelimitedItemIds);

					Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

					try {
						itemConnection.connect();
						itemConnection.executeQuery(getExternalIdItemBatchQuery);

						while (itemConnection.fetchNextRow()) {
							item = toItem(itemConnection);

							if (item != null) {
								items.add(item);
							}
						}
					} finally {
						if (itemConnection != null) {
							itemConnection.disconnect();
						}
					}
				}
			}
		}

		return items;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#addItemsBatch(java.util.Collection)
	 */
	@Override
	public Long addItemsBatch(Collection<Item> items) throws DataAccessException {
		Long addedItemCount = Long.valueOf(0);

		StringBuffer addItemsBatchQuery = new StringBuffer();

		addItemsBatchQuery
				.append("INSERT INTO `item` (`externalid`,`internalid`,`name`,`creatorname`,`price`,`source`,`type`,`added`,`country`,`currency`,`smallimage`,`mediumimage`,`largeimage`,`properties`) VALUES ");

		boolean addComma = false;
		for (Item item : items) {
			if (addComma) {
				addItemsBatchQuery.append(",");
			}

			addItemsBatchQuery.append(String.format("('%s','%s','%s','%s',%d,'%s','%s',FROM_UNIXTIME(%d),'%s','%s','%s','%s','%s','%s')",
					addslashes(item.externalId), addslashes(item.internalId), addslashes(item.name), addslashes(item.creatorName),
					(int) (item.price.floatValue() * 100.0f), addslashes(item.source), addslashes(item.type), item.added.getTime() / 1000,
					addslashes(item.country), addslashes(item.currency), addslashes(item.smallImage), addslashes(item.mediumImage),
					addslashes(item.largeImage), addslashes(item.properties)));
			addComma = true;
		}

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(addItemsBatchQuery.toString());

			if (itemConnection.getAffectedRowCount() > 0) {
				addedItemCount = Long.valueOf(itemConnection.getAffectedRowCount());
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return addedItemCount;
	}

	@Override
	public Long searchItemsCount(String query) throws DataAccessException {
		Long itemCount = Long.valueOf(0);

		if (itemCount == null) {
			String getDataAccountsCountQuery = String
					.format("SELECT count(1) AS `itemscount` FROM `item` WHERE (`externalid` LIKE '%%%1$s%%' OR `name` LIKE '%%%1$s%%' OR `creatorname` LIKE  '%%%1$s%%') AND `deleted`='n'",
							query);
			Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

			try {
				itemConnection.connect();
				itemConnection.executeQuery(getDataAccountsCountQuery);

				if (itemConnection.fetchNextRow()) {
					itemCount = itemConnection.getCurrentRowLong("itemscount");
				}
			} finally {
				if (itemConnection != null) {
					itemConnection.disconnect();
				}
			}
		}

		return itemCount;
	}

	@Override
	public List<Item> searchItems(String query, Pager pager) throws DataAccessException {

		List<Item> items = new ArrayList<Item>();
		String getDataItemQuery = String
				.format("SELECT * FROM `item` WHERE (`externalid` LIKE '%%%1$s%%' OR `name` LIKE '%%%1$s%%' OR `creatorname` LIKE '%%%1$s%%') AND `deleted`='n' ORDER BY `%2$s` %3$s LIMIT %4$d, %5$d",
						query, pager.sortBy == null ? "id" : pager.sortBy,
						pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC" : "DESC",
						pager.start == null ? Pager.DEFAULT_START.longValue() : pager.start.longValue(), pager.count == null ? Pager.DEFAULT_COUNT.longValue()
								: pager.count.longValue());

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getDataItemQuery);

			while (itemConnection.fetchNextRow()) {
				Item item = toItem(itemConnection);

				if (item != null) {
					items.add(item);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return items;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getInternalIdItemBatch(java.util.Collection)
	 */
	@Override
	public List<Item> getInternalIdItemBatch(Collection<String> itemIds) throws DataAccessException {
		List<Item> items = new ArrayList<Item>();

		String commaDelimitedItemIds = null;

		if (itemIds != null && itemIds.size() > 0) {
			commaDelimitedItemIds = StringUtils.join(itemIds, "','");

			if (commaDelimitedItemIds != null && commaDelimitedItemIds.length() != 0) {
				String getInternalIdItemBatchQuery = String.format(
						"SELECT * FROM `item` WHERE `internalid` IN ('%s') and `deleted`='n' GROUP BY `internalid`,`properties`", commaDelimitedItemIds);

				Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

				try {
					itemConnection.connect();
					itemConnection.executeQuery(getInternalIdItemBatchQuery);

					Map<String, Item> itemIdPropertiesLookup = new HashMap<String, Item>();
					Item item;
					while (itemConnection.fetchNextRow()) {
						item = toItem(itemConnection);

						if (item != null) {

							// If present, keep Item with properties not null
							if (itemIdPropertiesLookup.get(item.internalId) == null || itemIdPropertiesLookup.get(item.internalId).properties == null
									|| itemIdPropertiesLookup.get(item.internalId).properties.equals("null")) {
								itemIdPropertiesLookup.put(item.internalId, item);
							}
						}
					}

					items.addAll(itemIdPropertiesLookup.values());

				} finally {
					if (itemConnection != null) {
						itemConnection.disconnect();
					}
				}
			}

		}

		return items;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getItems(io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Item> getItems(Pager pager) throws DataAccessException {
		List<Item> items = new ArrayList<Item>();

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection itemConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getItemIdsQuery = "SELECT * FROM `item` WHERE `deleted`='n'";

		if (pager != null) {
			String sortByQuery = "id";

			if (pager.sortBy != null && ("internalid".equals(pager.sortBy) || "externalid".equals(pager.sortBy))) {
				sortByQuery = pager.sortBy;
			}

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

			getItemIdsQuery += String.format(" ORDER BY `%s` %s", sortByQuery, sortDirectionQuery);
		}

		if (pager.start != null && pager.count != null) {
			getItemIdsQuery += String.format(" LIMIT %d, %d", pager.start.longValue(), pager.count.longValue());
		} else if (pager.count != null) {
			getItemIdsQuery += String.format(" LIMIT %d", pager.count.longValue());
		}

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getItemIdsQuery);

			while (itemConnection.fetchNextRow()) {
				Item item = this.toItem(itemConnection);

				if (item != null) {
					items.add(item);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return items;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getItemsCount()
	 */
	@Override
	public Long getItemsCount() throws DataAccessException {
		Long itemsCount = Long.valueOf(0);

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getItemsCountQuery = "SELECT count(1) AS `itemcount` FROM `item` WHERE `deleted`='n'";

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getItemsCountQuery);

			if (itemConnection.fetchNextRow()) {
				itemsCount = itemConnection.getCurrentRowLong("itemcount");
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return itemsCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getDuplicateItemsInternalId(io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<String> getDuplicateItemsInternalId(Pager pager) throws DataAccessException {
		List<String> duplicateIntenalIds = new ArrayList<String>();

		final String getDuplicateItemsInternalIdQuery = String.format(
				// "SELECT `internalid` FROM `item` GROUP BY `internalid` HAVING COUNT(`internalid`) > 1 ORDER BY `%s` %s LIMIT %d,%d",
				// "SELECT `internalid`, count(1) as `count` FROM `item` WHERE `deleted`='n' GROUP BY `internalid` HAVING `count` > 1 ORDER BY `%s` %s LIMIT %d,%d",
				"SELECT `internalid`, count(1) as `count` FROM `item` GROUP BY `internalid` HAVING `count` > 1 ORDER BY `%s` %s LIMIT %d,%d",
				pager.sortBy == null ? "id" : pager.sortBy, pager.sortDirection == SortDirectionType.SortDirectionTypeDescending ? "DESC" : "ASC", pager.start,
				pager.count);

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getDuplicateItemsInternalIdQuery);

			String internalId;
			while (itemConnection.fetchNextRow()) {
				internalId = itemConnection.getCurrentRowString("internalid");

				if (internalId != null) {
					duplicateIntenalIds.add(internalId);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return duplicateIntenalIds;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getInternalIdItemAndDuplicates(java.lang.String)
	 */
	@Override
	public List<Item> getInternalIdItemAndDuplicates(String internalId) throws DataAccessException {
		List<Item> itemAndDuplicates = new ArrayList<Item>();

		final String getInternalIdItemAndDuplicatesQuery = String.format("SELECT * FROM `item` WHERE `internalid`='%s' AND `deleted`='n' ORDER BY `id` DESC",
				addslashes(internalId));

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getInternalIdItemAndDuplicatesQuery);

			while (itemConnection.fetchNextRow()) {
				Item item = toItem(itemConnection);

				if (item != null && "n".equals(item.deleted)) {
					itemAndDuplicates.add(item);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return itemAndDuplicates;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getPropertylessItemsCount()
	 */
	@Override
	public Long getPropertylessItemsCount() throws DataAccessException {
		Long propertylessItemsCount = Long.valueOf(0);

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getPropertylessItemsCountQuery = "SELECT count(1) AS `itemcount` FROM `item` WHERE (`properties`=null OR `properties`='null') AND `deleted`='n'";

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getPropertylessItemsCountQuery);

			if (itemConnection.fetchNextRow()) {
				propertylessItemsCount = itemConnection.getCurrentRowLong("itemcount");
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return propertylessItemsCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getPropertylessItems(io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Item> getPropertylessItems(Pager pager) throws DataAccessException {
		List<Item> propertylessItems = new ArrayList<Item>();

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getPropertylessItemsQuery = "SELECT * FROM `item` WHERE (`properties` IS NULL OR `properties`='null') AND `deleted`='n'";

		if (pager != null) {
			String sortByQuery = "id";

			if (pager.sortBy != null && ("internalid".equals(pager.sortBy) || "externalid".equals(pager.sortBy))) {
				sortByQuery = pager.sortBy;
			}

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

			getPropertylessItemsQuery += String.format(" ORDER BY `%s` %s", sortByQuery, sortDirectionQuery);
		}

		if (pager.start != null && pager.count != null) {
			getPropertylessItemsQuery += String.format(" LIMIT %d, %d", pager.start.longValue(), pager.count.longValue());
		} else if (pager.count != null) {
			getPropertylessItemsQuery += String.format(" LIMIT %d", pager.count.longValue());
		}

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getPropertylessItemsQuery);

			while (itemConnection.fetchNextRow()) {
				Item item = toItem(itemConnection);

				if (item != null) {
					propertylessItems.add(item);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return propertylessItems;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.item.IItemService#getPropertylessItemIds(io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Long> getPropertylessItemIds(Pager pager) throws DataAccessException {
		List<Long> propertylessItemIds = new ArrayList<Long>();

		Connection itemConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeItem.toString());

		String getPropertylessItemsQuery = "SELECT `id` FROM `item` WHERE (`properties` IS NULL OR `properties`='null') AND `deleted`='n'";

		if (pager != null) {
			String sortByQuery = "id";

			if (pager.sortBy != null && ("internalid".equals(pager.sortBy) || "externalid".equals(pager.sortBy))) {
				sortByQuery = pager.sortBy;
			}

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

			getPropertylessItemsQuery += String.format(" ORDER BY `%s` %s", sortByQuery, sortDirectionQuery);
		}

		if (pager.start != null && pager.count != null) {
			getPropertylessItemsQuery += String.format(" LIMIT %d, %d", pager.start.longValue(), pager.count.longValue());
		} else if (pager.count != null) {
			getPropertylessItemsQuery += String.format(" LIMIT %d", pager.count.longValue());
		}

		try {
			itemConnection.connect();
			itemConnection.executeQuery(getPropertylessItemsQuery);

			while (itemConnection.fetchNextRow()) {
				Long id = itemConnection.getCurrentRowLong("id");

				if (id != null) {
					propertylessItemIds.add(id);
				}
			}
		} finally {
			if (itemConnection != null) {
				itemConnection.disconnect();
			}
		}

		return propertylessItemIds;
	}
}