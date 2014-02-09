package com.csvfile.sorter.samples.sort;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

/**
 * The class tests that number sorting
 *
 */
public class SortOnlyNumberTest extends CsvFileSorterTest {
	
	private static final String SHORT_NUMBER_CSVFILE        = SAVE_DIRECTORY 
																+ F + "ShortNumber.csv";
	private static final String SORTED_SHORT_NUMBER_CSVFILE = SAVE_DIRECTORY 
																+ F + "SortedShortNumber.csv";
	private static final String VERIFY_SHORT_NUMBER_CSVFILE = SAVE_DIRECTORY 
																+ F + "VerifyShortNumber.csv";

	private static final String LONG_NUMBER_CSVFILE        = SAVE_DIRECTORY 
																+ F + "LongNumber.csv";
	private static final String SORTED_LONG_NUMBER_CSVFILE = SAVE_DIRECTORY 
																+ F + "SortedLongNumber.csv";
	private static final String VERIFY_LONG_NUMBER_CSVFILE = SAVE_DIRECTORY 
																+ F + "VerifyLongNumber.csv";

	/**
	 * This method sorts sort number
	 * 
	 * before sort
	 * {"7", "8", "9"}
	 * {"5", "7", "8"}
	 * {"1", "2", "4"}
	 * {"1", "2", "3"}
	 * {"5", "6", "7"}
	 * 
	 * after sort
	 * {"1", "2", "3"}
	 * {"1", "2", "4"}
	 * {"5", "6", "7"}
	 * {"5", "7", "8"}
	 * {"7", "8", "9"}
	 * 
	 * @throws IOException
	 */
	@Test
	public void ShortNumberIsSortedInAscendingOrder() throws IOException {

		// Test data is created
		String[][] testData = {{"7", "8", "9"},
							   {"5", "7", "8"},
							   {"1", "2", "4"},
							   {"1", "2", "3"},
							   {"5", "6", "7"}};

		// Expected data is created
		String[][] expectedData = {{"1", "2", "3"},
								   {"1", "2", "4"},
								   {"5", "6", "7"},
								   {"5", "7", "8"},
								   {"7", "8", "9"}};

		// Test file is generated
		generateTestFile( generateTestData(testData), SHORT_NUMBER_CSVFILE );

		// action
		CsvFileSorter.sort( SHORT_NUMBER_CSVFILE, SORTED_SHORT_NUMBER_CSVFILE );

		// verifying test result
		List<List<String>> sortedTestData = readTestFile( SORTED_SHORT_NUMBER_CSVFILE );

		assertTrue( sortedTestData.equals( generateTestData(expectedData) ));

		// if you don't need, keep the comment
		verifyCSVTestFile( sortedTestData, VERIFY_SHORT_NUMBER_CSVFILE );
	}

	/**
	 * This method sorts sort number
	 * 
	 * before sort
	 * {"7262462512", "8735735.6763836837", "9452145"},
	 * {"54251255", "741264124.642412", "8.0000000000"},
	 * {"54251255", "741264124.642412", "8"},
	 * {"13413516341", "2341433", "0.523454"},
	 * {"13413516341", "2341433", "0.5234540000"},
	 * {"0", "25153", "000000"},
	 * {"0", "25153", "000"},
	 * {"52478594", "0.0000524525", "7.0452400054"},
	 * {"52478594", "0.00005245", "7.0452400054"}};
	 * 
	 * after sort
	 * {"0","25153","000"}
	 * {"0","25153","000000"}
	 * {"13413516341","2341433","0.523454"}
	 * {"13413516341","2341433","0.5234540000"}
	 * {"52478594","0.00005245","7.0452400054"}
	 * {"52478594","0.0000524525","7.0452400054"}
	 * {"54251255","741264124.642412","8"}
	 * {"54251255","741264124.642412","8.0000000000"}
	 * {"7262462512","8735735.6763836837","9452145"}
	 * 
	 * @throws IOException
	 */
	@Test
	public void LongNumberIsSortedInAscendingOrder() throws IOException {

		// Test data is created
		String[][] testData = {{"7262462512", "8735735.6763836837", "9452145"},
							   {"54251255", "741264124.642412", "8.0000000000"},
							   {"54251255", "741264124.642412", "8"},
							   {"13413516341", "2341433", "0.523454"},
							   {"13413516341", "2341433", "0.5234540000"},
							   {"0", "25153", "000000"},
							   {"0", "25153", "000"},
							   {"52478594", "0.0000524525", "7.0452400054"},
							   {"52478594", "0.00005245", "7.0452400054"}};

		// Expected data is created
		String[][] expectedData = {{"0","25153","000"},
								   {"0","25153","000000"},
								   {"13413516341","2341433","0.523454"},
								   {"13413516341","2341433","0.5234540000"},
								   {"52478594","0.00005245","7.0452400054"},
								   {"52478594","0.0000524525","7.0452400054"},
								   {"54251255","741264124.642412","8"},
								   {"54251255","741264124.642412","8.0000000000"},
								   {"7262462512","8735735.6763836837","9452145"}};

		// Test file is generated
		generateTestFile( generateTestData(testData), LONG_NUMBER_CSVFILE );

		// action
		CsvFileSorter.sort( LONG_NUMBER_CSVFILE, SORTED_LONG_NUMBER_CSVFILE );

		// verifying test result
		List<List<String>> sortedTestData = readTestFile( SORTED_LONG_NUMBER_CSVFILE );

		assertTrue( sortedTestData.equals( generateTestData(expectedData) ));

		// if you don't need, keep the comment
		verifyCSVTestFile( sortedTestData, VERIFY_LONG_NUMBER_CSVFILE );
	}
}
