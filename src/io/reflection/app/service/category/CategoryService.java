//  
//  CategoryService.java
//  reflection.io
//
//  Created by William Shakour on January 31, 2014.
//  Copyright © 2014 SPACEHOPPER STUDIOS LTD. All rights reserved.
//  Copyright © 2014 reflection.io. All rights reserved.
//
package io.reflection.app.service.category;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.api.shared.datatypes.Pager;
import io.reflection.app.api.shared.datatypes.SortDirectionType;
import io.reflection.app.datatypes.shared.Category;
import io.reflection.app.datatypes.shared.Store;
import io.reflection.app.repackaged.scphopr.cloudsql.Connection;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseServiceProvider;
import io.reflection.app.repackaged.scphopr.service.database.DatabaseType;
import io.reflection.app.repackaged.scphopr.service.database.IDatabaseService;
import io.reflection.app.service.ServiceType;

import java.util.ArrayList;
import java.util.List;

final class CategoryService implements ICategoryService {
	public String getName() {
		return ServiceType.ServiceTypeCategory.toString();
	}

	@Override
	public Category getCategory(Long id) throws DataAccessException {
		Category category = null;

		IDatabaseService databaseService = DatabaseServiceProvider.provide();
		Connection categoryConnection = databaseService.getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		String getCategoryQuery = String.format("SELECT * FROM `category` WHERE `deleted`='n' AND `id`=%d LIMIT 1", id.longValue());
		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getCategoryQuery);

			if (categoryConnection.fetchNextRow()) {
				category = toCategory(categoryConnection);
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}
		return category;
	}

	/**
	 * To category
	 * 
	 * @param connection
	 * @return
	 */
	private Category toCategory(Connection connection) throws DataAccessException {
		Category category = new Category();

		category.id = connection.getCurrentRowLong("id");
		category.created = connection.getCurrentRowDateTime("created");
		category.deleted = connection.getCurrentRowString("deleted");

		category.parent = new Category();
		category.parent.id = connection.getCurrentRowLong("parentid");

		category.internalId = connection.getCurrentRowLong("internalid");
		category.name = connection.getCurrentRowString("name");
		category.store = connection.getCurrentRowString("store");

		return category;
	}

	@Override
	public Category addCategory(Category category) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Category updateCategory(Category category) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteCategory(Category category) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getStoreCategoriesCount(io.reflection.app.datatypes.shared.Store)
	 */
	@Override
	public Long getStoreCategoriesCount(Store store) throws DataAccessException {
		Long categoriesCount = Long.valueOf(0);

		String getStoreCategoriesCountQuery = String.format("SELECT count(1) as `categoriescount` FROM `category` WHERE `store`='%s' AND `deleted`='n'",
				store.a3Code);

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getStoreCategoriesCountQuery);

			if (categoryConnection.fetchNextRow()) {
				categoriesCount = categoryConnection.getCurrentRowLong("categoriescount");
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}

		return categoriesCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getStoreCategories(io.reflection.app.datatypes.shared.Store,
	 * io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Category> getStoreCategories(Store store, Pager pager) throws DataAccessException {
		List<Category> categories = new ArrayList<Category>();

		String getStoreCategoriesQuery = String.format("SELECT * FROM `category` WHERE `store`='%s' AND `deleted`='n' ORDER BY `%s` %s LIMIT %d, %d",
				store.a3Code, pager.sortBy == null ? "id" : pager.sortBy, pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC" : "DESC",
				pager.start == null ? Pager.DEFAULT_START.longValue() : pager.start.longValue(), pager.count == null ? Pager.DEFAULT_COUNT.longValue()
						: pager.count.longValue());

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getStoreCategoriesQuery);

			while (categoryConnection.fetchNextRow()) {
				Category category = toCategory(categoryConnection);

				if (category != null) {
					categories.add(category);
				}
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}

		return categories;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getParentCategories(io.reflection.app.datatypes.shared.Category,
	 * io.reflection.app.api.shared.datatypes.Pager)
	 */
	@Override
	public List<Category> getParentCategories(Category parent, Pager pager) throws DataAccessException {
		List<Category> categories = new ArrayList<Category>();

		String getStoreParentCategoriesQuery = String.format("SELECT * FROM `category` WHERE `parentid`=%d AND `deleted`='n' ORDER BY `%s` %s LIMIT %d, %d",
				parent.id.longValue(), pager.sortBy == null ? "id" : pager.sortBy, pager.sortDirection == SortDirectionType.SortDirectionTypeAscending ? "ASC"
						: "DESC", pager.start == null ? Pager.DEFAULT_START.longValue() : pager.start.longValue(),
				pager.count == null ? Pager.DEFAULT_COUNT.longValue() : pager.count.longValue());

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getStoreParentCategoriesQuery);

			while (categoryConnection.fetchNextRow()) {
				Category category = toCategory(categoryConnection);

				if (category != null) {
					categories.add(category);
				}
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}

		return categories;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getParentCategoriesCount(io.reflection.app.datatypes.shared.Category)
	 */
	@Override
	public Long getParentCategoriesCount(Category parent) throws DataAccessException {
		Long categoriesCount = Long.valueOf(0);

		String getStoreParentCategoriesQuery = String.format("SELECT count(1) as `categoriescount` FROM `category` WHERE `deleted`='n' AND `parentid`=%d",
				parent.id.longValue());

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getStoreParentCategoriesQuery);

			if (categoryConnection.fetchNextRow()) {
				categoriesCount = categoryConnection.getCurrentRowLong("categoriescount");
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}

		return categoriesCount;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getAllCategory(io.reflection.app.datatypes.shared.Store)
	 */
	@Override
	public Category getAllCategory(Store store) throws DataAccessException {
		Category category = null;

		String getAllCategoryQuery = String
				.format("SELECT * FROM `category` WHERE `deleted`='n' AND `store`='%s' AND `parentid` IS NULL LIMIT 1", store.a3Code);

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getAllCategoryQuery);

			if (categoryConnection.fetchNextRow()) {
				category = toCategory(categoryConnection);
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}
		return category;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.service.category.ICategoryService#getInternalIdCategory(io.reflection.app.datatypes.shared.Store, java.lang.Long)
	 */
	@Override
	public Category getInternalIdCategory(Store store, Long internalId) throws DataAccessException {
		Category category = null;

		String getInternalIdCategoryQuery = String.format("SELECT * FROM `category` WHERE `deleted`='n' AND `store`='%s' AND `internalid`=%d LIMIT 1",
				store.a3Code, internalId.longValue());

		Connection categoryConnection = DatabaseServiceProvider.provide().getNamedConnection(DatabaseType.DatabaseTypeCategory.toString());

		try {
			categoryConnection.connect();
			categoryConnection.executeQuery(getInternalIdCategoryQuery);

			if (categoryConnection.fetchNextRow()) {
				category = toCategory(categoryConnection);
			}
		} finally {
			if (categoryConnection != null) {
				categoryConnection.disconnect();
			}
		}

		return category;
	}
}