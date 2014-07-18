/**
 * 
 */
package io.reflection.app.collectors;

import io.reflection.app.logging.GaeLevel;

import java.util.List;
import java.util.logging.Logger;

/**
 * @author William Shakour
 * 
 */
public class CollectorAmazon implements Collector {

	private static final Logger LOG = Logger.getLogger(CollectorAmazon.class.getName());

	@Override
	public List<Long> collect(String country, String type, String category, Long code) {
		// <?php
		//
		// //Enter your IDs
		// define("Access_Key_ID", "[Your Access Key ID]");
		// define("Associate_tag", "[Your Associate Tag ID]");
		//
		// //Set up the operation in the request
		// function ItemSearch($SearchIndex, $Keywords){
		//
		// //Set the values for some of the parameters
		// $Operation = "ItemSearch";
		// $Version = "2011-08-01";
		// $ResponseGroup = "ItemAttributes,Offers";
		// //User interface provides values
		// //for $SearchIndex and $Keywords
		//
		// //Define the request
		// $request=
		// "http://webservices.amazon.com/onca/xml"
		// . "?Service=AWSECommerceService"
		// . "&AssociateTag=" . Associate_tag
		// . "&AWSAccessKeyId=" . Access_Key_ID
		// . "&Operation=" . $Operation
		// . "&Version=" . $Version
		// . "&SearchIndex=" . $SearchIndex
		// . "&Keywords=" . $Keywords
		// . "&Signature=" . [Request Signature]
		// . "&ResponseGroup=" . $ResponseGroup;
		//
		// //Catch the response in the $response object
		// $response = file_get_contents($request);
		// $parsed_xml = simplexml_load_string($response);
		// printSearchResults($parsed_xml, $SearchIndex);
		// }
		// ?>

		if (LOG.isLoggable(GaeLevel.TRACE)) {
			LOG.log(GaeLevel.TRACE, "Entering...");
		}

		try {

		} finally {
			if (LOG.isLoggable(GaeLevel.TRACE)) {
				LOG.log(GaeLevel.TRACE, "Exiting...");
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.collectors.Collector#isGrossing(java.lang.String)
	 */
	@Override
	public boolean isGrossing(String type) {
		throw new UnsupportedOperationException();
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
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.reflection.app.collectors.Collector#getTypes()
	 */
	@Override
	public List<String> getTypes() {
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
