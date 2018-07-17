package runnableClasses;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import edu.buffalo.www.cse4562.QuerySolver;
import edu.buffalo.www.cse4562.ReadTable;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import types.JavaTypes;
import types.TableStats;
import types.TupleRecord;

public class TableAttributes implements TreeNode {
	//Tuple<String, HashMap<String, List<String>>, List<String>>
	String tableName;
	private String tableAlias = null;
	//Table schemaTable;
	ReadTable table;

	static int block = 1;
	int counter = 0, ask = 0;

	TableStats stats;
	boolean first = true; 
	private int[] type;

	public TableAttributes(String tableName, String alias){
		this.tableName = tableName;
		this.tableAlias = alias;

		type = QuerySolver.schemaCollectionType.get(tableName);

		try {
			table = new ReadTable(tableName, '|');	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch blTableAttributesock
			e.printStackTrace();
		}

		stats = QuerySolver.statitics.get(tableName);

		first = true;
	}

	@Override
	public void open(Column primary) {

	}


	@Override
	public boolean hasNext() {
		if(ask < counter)
			return true;
		return table.hasNext();
	}

	//	HashMap<String, TupleRecord[]> hashMap = new HashMap<>();
	TupleRecord[] tuple = new TupleRecord[block];

	PrimitiveValue previouslyAsked = null;
	//	Iterator<PrimitiveValue> previousIterator = null;
	Iterator<TupleRecord> previousTupleRecord = null;

	HashMap<PrimitiveValue, Long> hashMap;
	ObjectInputStream file = null;
	//	FileInputStream fileInput = null;
	//	File fileI = null;

	RandomAccessFile fileI = null;

	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {
		// TODO Auto-generated method stub

		//		if(ask < counter)
		//			return true;
		if(condition == null) {
			if(counter == 0 || ask >= counter) {
				ask = 0;
				//tuple = new TupleRecord[block];
				for(counter = 0;counter < block; counter++) {
					tuple[counter] = table.nextTuple();

					if(tuple[counter] == null) {
						break;
					}
					//				System.err.println("Collecting === " + " -=-=-= counter is " + counter);
					//				for(PrimitiveValue primitiveValue : tuple[counter])
					//					System.err.print(primitiveValue + " | ");
					//				System.err.println();


				}
			}

			//		System.err.println("Returnig ---- for asked === " + ask + " -=-=-= counter is " + counter);
			//		for(PrimitiveValue primitiveValue : tuple[ask])
			//			System.err.print(primitiveValue + " | ");
			//		System.err.println();

			if(ask >= counter)
				return null;

			//			System.err.println(" returning from left side " + tuple[ask].getAll());

			return tuple[ask++];
		}
		else {

			if(first) {
				try {
					fileI = new RandomAccessFile("data/" + tableName + "/" + stats.primary,"r");
//					fr = new FileReader(fileI.getFD());
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				hashMap = stats.primaryIndex;
				first = false;
				
				 

			}

			return readPrimaryValue(condition, fileI);

		}


	}

	private PrimitiveValue getPrimitive(String toBeConverted, int colDataType) {

		switch (colDataType) {

		case JavaTypes.StringValue:
			return new StringValue(toBeConverted);

		case JavaTypes.LongValue:
			return new LongValue(toBeConverted);

		case JavaTypes.DoubleValue:
			return new DoubleValue(toBeConverted);

		case JavaTypes.DateValue:
			return new DateValue(toBeConverted);

		default:
			break;
		}

		return new LongValue(55555555);
	}

	BufferedReader br = null;
	private TupleRecord readPrimaryValue(PrimitiveValue request, RandomAccessFile fileI){
		try {
			//			Long position = request.toLong() * 100;
			//			if(stats.primarySet.contains(position)) {
			if(hashMap.containsKey(request)) {
				//				fileI.seek(position);
				fileI.seek(hashMap.get(request));
				
//				br = new BufferedReader(fr);
//				String read = br.readLine();

				String read = fileI.readUTF();
				//				System.err.println(" before reading it :  " + read );
				//				String[] list = read.split("\\|");
				StringTokenizer list = new StringTokenizer(read, "|"); 
				//				System.err.println(" the splitted output is " + Arrays.asList(list) );
				List<PrimitiveValue> primitiveValues = new ArrayList<PrimitiveValue>();

				int j = 0;
				//				for(String data : list){
				//					primitiveValues.add(getPrimitive(data, type[j]));
				//					j++;
				//				}
				while(list.hasMoreTokens()){
					primitiveValues.add(getPrimitive(list.nextToken(), type[j]));
					j++;
				}

				TupleRecord record = new TupleRecord(primitiveValues);
				return record;

			}
//			File file = new File("data/" + tableName + "/" + stats.primary + request);
			
//			if(hashMap.containsKey(request)) {
//				
////				fileI = new RandomAccessFile(file, "r");
////				BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
////				BufferedInputStream bufferedReader = new BufferedInputStream(new FileInputStream("data/" + tableName + "/" + stats.primary + hashMap.get(request)));
//				
////				String read = fileI.readUTF();
////				String read = bufferedReader.readLine();
//				
//				ObjectInputStream objectInputStream = new ObjectInputStream( new BufferedInputStream(new FileInputStream("data/" + tableName + "/" + stats.primary + hashMap.get(request))) );
//				String read = (String)objectInputStream.readObject();
//				
////				byte[] contents = new byte[1024];
////
////				int bytesRead = 0;
////				String read = "";
////				while((bytesRead = bufferedReader.read(contents)) != -1) { 
////					read += new String(contents, 0, bytesRead);              
////				}
//				
////				String read = strFileContents;
//				objectInputStream.close();
//				//				System.err.println(" before reading it :  " + read );
//				//				String[] list = read.split("\\|");
//				StringTokenizer list = new StringTokenizer(read, "|"); 
//				//				System.err.println(" the splitted output is " + Arrays.asList(list) );
//				List<PrimitiveValue> primitiveValues = new ArrayList<PrimitiveValue>();
//
//				int j = 0;
//				//				for(String data : list){
//				//					primitiveValues.add(getPrimitive(data, type[j]));
//				//					j++;
//				//				}
//				while(list.hasMoreTokens()){
//					primitiveValues.add(getPrimitive(list.nextToken(), type[j]));
//					j++;
//				}
//
//				TupleRecord record = new TupleRecord(primitiveValues);
//				return record;
//
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		if(tableAlias != null) {
			Column[] columns = QuerySolver.schemaCollection.get(tableName);

			//			System.out.println(columns[0].getWholeColumnName() + " len is " + columns.length);

			Column[] columnsAlias = new Column[columns.length];

			//			System.out.println(columnsAlias.length);

			int i = 0;
			for(Column column: columns) {
				//		System.out.println( i + "   " + columnsAlias[i]);
				columnsAlias[i++ ] = new Column(new Table(tableAlias), column.getColumnName());
				//				/columnsAlias[i++].setColumnName(column.getColumnName());
			}

			//System.out.println("Returning " + columnsAlias[0].getWholeColumnName());

			QuerySolver.schemaCollection.put(tableAlias, columnsAlias);

			return columnsAlias;
		}

		//System.out.println("Returning " + QuerySolver.schemaCollection.get(tableName)[0].getWholeColumnName());

		return QuerySolver.schemaCollection.get(tableName);
	}

	@Override
	public void restart() {
		//System.out.println("Restart Called");
		ask = counter = 0;
		//ask = 0;
		//table.close();
		//table = new ReadTable(tableName, '|');
		table.restart();
		first = true;

		//		previouslyAsked = null;
		//		previousIterator = null;
		previousTupleRecord = null;
	}

	@Override
	public TreeNode getChild() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setChild(TreeNode node) {
		// TODO Auto-generated method stub
		//this. = node;
	}

	@Override
	public List<Column> getColumn() {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean optimized = false;

	public boolean isOptimized() {
		return optimized;
	}

	public void setOptimized(boolean optimized) {
		this.optimized = optimized;
	}

	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		//		return 0;
		return stats.totalCount;
	}

}