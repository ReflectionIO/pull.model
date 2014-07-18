/**
 * 
 */
package io.reflection.app.collectors;

import io.reflection.app.api.exception.DataAccessException;
import io.reflection.app.logging.GaeLevel;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author billy1380
 * 
 */
public class CollectorIOS implements Collector {

	private static final Logger LOG = Logger.getLogger(CollectorIOS.class.getName());

	public static final String TOP_FREE_APPS = "topfreeapplications";
	public static final String TOP_PAID_APPS = "toppaidapplications";
	public static final String TOP_GROSSING_APPS = "topgrossingapplications";
	public static final String TOP_FREE_IPAD_APPS = "topfreeipadapplications";
	public static final String TOP_PAID_IPAD_APPS = "toppaidipadapplications";
	public static final String TOP_GROSSING_IPAD_APPS = "topgrossingipadapplications";

	// public static final String NEW_APPS = "newapplications";
	// public static final String NEW_FREE_APPS = "newfreeapplications";
	// public static final String NEW_PAID_APPS = "newpaidapplications";

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.collectors.Collector#isGrossing(java.lang.String)
	 */
	@Override
	public boolean isGrossing(String type) {
		return type.equalsIgnoreCase(TOP_GROSSING_IPAD_APPS) || type.equalsIgnoreCase(TOP_GROSSING_APPS);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * io.reflection.app.collectors.Collector#getCounterpartTypes(java.lang.
	 * String)
	 */
	@Override
	public List<String> getCounterpartTypes(String type) {
		List<String> counterParts = null;

		if (LOG.isLoggable(GaeLevel.DEBUG)) {
			LOG.finer(type);
		}

		switch (type) {
		case TOP_FREE_APPS:
			counterParts = Arrays.asList(TOP_GROSSING_APPS);
		case TOP_PAID_APPS:
			counterParts = Arrays.asList(TOP_GROSSING_APPS);
		case TOP_GROSSING_APPS:
			counterParts = Arrays.asList(TOP_PAID_APPS, TOP_FREE_APPS);
		case TOP_FREE_IPAD_APPS:
			counterParts = Arrays.asList(TOP_GROSSING_IPAD_APPS);
		case TOP_PAID_IPAD_APPS:
			counterParts = Arrays.asList(TOP_GROSSING_IPAD_APPS);
		case TOP_GROSSING_IPAD_APPS:
			counterParts = Arrays.asList(TOP_PAID_IPAD_APPS, TOP_FREE_IPAD_APPS);
		}

		if (LOG.isLoggable(GaeLevel.DEBUG)) {
			if (counterParts == null) {

			} else {
				for (String counterPart : counterParts) {
					LOG.finer(counterPart);
				}
			}
		}

		return counterParts;
	}

	@Override
	public List<String> getTypes() {
		return Arrays.asList(TOP_FREE_APPS, TOP_PAID_APPS, TOP_GROSSING_APPS, TOP_FREE_IPAD_APPS, TOP_PAID_IPAD_APPS, TOP_GROSSING_IPAD_APPS);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.collectors.Collector#collect(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.Long)
	 */
	@Override
	public List<Long> collect(String country, String type, String category, Long code) throws DataAccessException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.collectors.Collector#enqueue()
	 */
	@Override
	public int enqueue() {
		throw new UnsupportedOperationException();
	}

}
