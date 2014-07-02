/**
 * 
 */
package io.reflection.app.collectors;

import io.reflection.app.api.exception.DataAccessException;

import java.util.List;

/**
 * @author billy1380
 * 
 */
public interface Collector {

	public static final String ENQUEUE_GATHER_FORMAT = "/gather?store=%s&country=%s&type=%s&code=%d";
	public static final String ENQUEUE_GATHER_CATEGORY_FORMAT = "/gather?store=%s&country=%s&type=%s&category=%d&code=%d";

	/**
	 * Collects data for
	 * 
	 * @param country
	 *            the store country
	 * @param type
	 *            top paid, top free top grossing etc
	 * @param category
	 *            the category id for items (in db terms this is the internal id
	 *            in the category column)
	 * @param code
	 *            ties all items in a single gather such that top grossing and
	 *            top paid and top free entries can be married
	 */
	List<Long> collect(String country, String type, String category, Long code) throws DataAccessException;

	/**
	 * Puts a message in the queue for each list for each country
	 * 
	 * @return The number of messages added
	 */
	int enqueue();

	boolean isGrossing(String type);

	/**
	 * @param type
	 * @return
	 */
	List<String> getCounterpartTypes(String type);

	/**
	 * @return
	 */
	List<String> getTypes();
}
