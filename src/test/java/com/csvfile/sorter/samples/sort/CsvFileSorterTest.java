package com.csvfile.sorter.samples.sort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.csvfile.sorter.samples.serialize.ListOfStringSerializer;

/**
 * Utility Class for junit test
 *
 */
public class CsvFileSorterTest {
	
	protected static final String F = File.separator;
	
	protected static final String SAVE_DIRECTORY = "src" + F + "test" + F + "resources";
	
	/**
	 * This method makes csv file that test result for verifly
	 * 
	 * @param rowsData
	 * @param file
	 * @throws FileNotFoundException
	 */
	protected void verifyCSVTestFile( List<List<String>> rowsData, String file ) throws FileNotFoundException {

		PrintWriter pw = new PrintWriter(
							new BufferedOutputStream(
								new FileOutputStream(
									new File( file ))));
		try {
			for( List<String> row : rowsData ) {
				for( String cell : row ) {
					pw.write(cell);
					pw.write(",");
				}
				pw.println();
			}
		} finally {
			pw.flush();
			pw.close();
		}
	}

	/**
	 * This method reads file that is created in test
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	protected List<List<String>> readTestFile( String file ) throws IOException {

		ObjectInputStream ois = new ObjectInputStream(
									new BufferedInputStream(
										new SnappyInputStream(
											new FileInputStream( file ))));

		List<List<String>> rowsData = new ArrayList<List<String>>();
		List<String> row = new ArrayList<String>();
		try {
			while( (row = ((ListOfStringSerializer) ois.readObject()).getData())
				   !=
				   null ) {
				rowsData.add(row);
			}
		} catch( EOFException eof ) {
			// End of File
		} catch (ClassNotFoundException e) {
			// There is no possibility of other class cast.
			e.printStackTrace();
		} finally {
			ois.close();
		}

		return rowsData;
	}

	/**
	 * This method makes file in using test.
	 * 
	 * @param testData
	 * @param file
	 * @throws IOException
	 */
	protected void generateTestFile(List<List<String>> testData, String file) throws IOException {

		ObjectOutputStream oos = new ObjectOutputStream(
									new BufferedOutputStream(
										new SnappyOutputStream(
											new FileOutputStream(
												new File( file )))));

		try {
			for( List<String> data : testData ) {
				oos.writeObject( new ListOfStringSerializer(data) );
			}
		} finally {
			oos.flush();
			oos.close();
		}
	}
	
	/**
	 * This method makes test data.
	 * 
	 * @param data
	 * @return test data in type of List<List<String>>
	 */
	protected List<List<String>> generateTestData(String[][] data) {
		List<List<String>> rowsList = new ArrayList<List<String>>();
		for( int i=0; i < data.length; i++) {
			List<String> row = new ArrayList<String>();
			for( int k=0; k < data[i].length; k++) {
				row.add(data[i][k]);
			}
			rowsList.add(row);
		}
		return rowsList;
	}
}
