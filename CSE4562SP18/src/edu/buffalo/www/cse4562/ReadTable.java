//csv file read
package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import types.TupleRecord;

public class ReadTable {


	private String tableFileName;
	private Character delimiter;

	private int[] type;

	Iterator<CSVRecord> parser;
	private CSVParser csv;
	
	public ReadTable(String tableFileName, Character delimiter) throws FileNotFoundException {
		// TODO Auto-generated constructor stub
		this.tableFileName = "data/" + tableFileName + ".dat";
		this.delimiter = delimiter;


		type = QuerySolver.schemaCollectionType.get(tableFileName);

		try {
			BufferedReader reader = Files.newBufferedReader(Paths.get(this.tableFileName), StandardCharsets.UTF_8);
			csv = new CSVParser( reader , CSVFormat.newFormat(this.delimiter));
			parser = csv.iterator();//.parse(this.bufferedReader).iterator();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	private TupleRecord readLine() throws IOException {
		
		if(!parser.hasNext()) {
			csv.close();
			return null;
		}
		CSVRecord record = parser.next();
		
		return new TupleRecord(record, type);
	}

	public TupleRecord nextTuple() {
		try {
			return readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean hasNext() {
		return parser.hasNext();
	}
	
	int rscount = 0;
	public void restart() {
		try {
			csv = new CSVParser( new FileReader( new File(this.tableFileName) ) , CSVFormat.newFormat(this.delimiter));
			parser = csv.iterator();//.parse(this.bufferedReader).iterator();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}