package edu.buffalo.www.cse4562;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import optimizers.Optimize;
import optimizers.OptimizeProjection;
import projectionsClasses.GroupBy;
import projectionsClasses.Projection;
import projectionsClasses.ProjectionAggregate;
import runnableClasses.JoinProduct;
import runnableClasses.LimitQuery;
import runnableClasses.OrderBy;
import runnableClasses.Selection;
import runnableClasses.TableAttributes;
import runnableClasses.TreeNode;
import types.JavaTypes;
import types.TableStats;
import types.TupleRecord;

public class QuerySolver {

	public static final int SELECT = 1000;
	public static final int CREATETABLE = 2000;
	public static final int INSERT = 3000;

	private int queryType = new Integer(0);

	Statement statement; 

	public static HashMap<String, Column[]> schemaCollection = new HashMap<String, Column[]>();
	public static HashMap<String, int[]> schemaCollectionType = new HashMap<String, int[]>();
	public static HashMap<String, TableStats> statitics = new HashMap<String, TableStats>();

	public QuerySolver(Statement statement) {
		this.statement = statement;
	}

	TupleRecord record;

	public TreeNode buildPlan() throws ParseException {
		Error error = new Error("Cannot resolve the query");

		TreeNode node = null;


		if (this.statement instanceof Select) {

			setQueryType(SELECT);

			Select select = (Select) this.statement;
			SelectBody body = select.getSelectBody();


			if (body instanceof PlainSelect) {
				PlainSelect plain = (PlainSelect)body;

				FromItem fromItem = plain.getFromItem();

				ExecuteFromItem executeFromItem = new ExecuteFromItem();

				fromItem.accept(executeFromItem);

				node = executeFromItem.getNode();


				List<Join> joins = plain.getJoins();

				if(joins != null) {
					for(Join join : joins) {
						node = new JoinProduct(node, join);
					}
				}


				//				List<Join> joins = plain.getJoins();
				//
				//				if(joins != null) {
				//					FromItem fromItem = plain.getFromItem();
				//					ExecuteFromItem executeFromItem = new ExecuteFromItem();
				//					fromItem.accept(executeFromItem);
				//					TreeNode firstNode = executeFromItem.getNode();
				//
				//					//					List<FromItem> fromItems = new ArrayList<>();
				//					//					fromItems.add(fromItem);
				//
				//					HashMap<TreeNode, FromItem> fromItemsMapper = new HashMap<>();
				//
				//					List<TreeNode> temp = new ArrayList<>();
				//					temp.add(firstNode);
				//					fromItemsMapper.put(firstNode, fromItem);
				//
				//					for(Join join : joins) {
				//						join.getRightItem().accept(executeFromItem);
				//						temp.add( executeFromItem.getNode() );
				//						fromItemsMapper.put(executeFromItem.getNode(), join.getRightItem());
				//					}
				//
				//					//					Collections.sort(temp, new Comparator<TreeNode>() {
				//					//						@Override
				//					//						public int compare(TreeNode o1, TreeNode o2) {
				//					//							// TODO Auto-generated method stub
				//					//							int v1 = o1.getEstimate(), v2 = o2.getEstimate();
				//					//							return v1 - v2;
				//					//						}
				//					//					});
				//
				//					List<Join> leftDeepJoin = new ArrayList<>();
				//
				//					Iterator<TreeNode > iterator = temp.iterator();
				//					node = iterator.next();
				//
				//					System.err.println(" The count of joins " + node.getEstimate());
				//
				//					while(iterator.hasNext()) {
				//						TreeNode treeNode = iterator.next();
				//
				//						System.err.println(" The count of joins " + treeNode.getEstimate());
				//						Join join = new Join();
				//						join.setRightItem(fromItemsMapper.get(treeNode));
				//						leftDeepJoin.add(join);
				//					}
				//
				//					for(Join join : leftDeepJoin) {
				//						System.err.println("The count for " + node + " is : " + node.getEstimate());
				//						node = new JoinProduct(node, join);
				//					}
				//				}
				//				else {
				//					FromItem fromItem = plain.getFromItem();
				//					ExecuteFromItem executeFromItem = new ExecuteFromItem();
				//					fromItem.accept(executeFromItem);
				//					node = executeFromItem.getNode();
				//				}

				Expression expression = plain.getWhere();

				if(expression != null) {
					node = new Selection(node, expression);
				}

				List<Column> groupByCol = plain.getGroupByColumnReferences();
				//System.out.println(groupByCol);
				if(groupByCol != null) {
					node = new GroupBy(node, groupByCol, plain.getSelectItems());
				}
				else  {
					Boolean isFun = false;
					for(SelectItem item : plain.getSelectItems()) {
						if(item instanceof SelectExpressionItem) {
							SelectExpressionItem expressionItem = (SelectExpressionItem)item;
							if(expressionItem.getExpression() instanceof Function) {
								isFun = true;
								node = new ProjectionAggregate(node, plain.getSelectItems());
								break;
							}
						}
					}
					if(!isFun)
						node = new Projection(node, plain.getSelectItems());
				}

				List<OrderByElement> orderByElements = plain.getOrderByElements();
				if(orderByElements != null && !orderByElements.isEmpty()){
					node = new OrderBy(node, orderByElements);
				}

				if(plain.getLimit() != null)
					node = new LimitQuery(node, plain.getLimit());

			}
			else {
				throw error;
			}
		}
		else if (this.statement instanceof CreateTable) {

			setQueryType(CREATETABLE);

			CreateTable create = (CreateTable) this.statement;
			String tableName = create.getTable().getName();

			List<Column> columnIndex  = new ArrayList<>();
			List<Column> columnRef  = new ArrayList<>();
			List<ColumnDefinition> columnDefinition = create.getColumnDefinitions();
			TableStats stats = new TableStats();

			Column[] columns = new Column[columnDefinition.size()];
			int[] type = new int[columnDefinition.size()];

			int i = 0;
			HashMap<Column, Integer> schemaWhole = new HashMap<>();

			for (ColumnDefinition columnDefinition2 : columnDefinition) {
				columns[i] = new Column(create.getTable(), columnDefinition2.getColumnName());
				schemaWhole.put(columns[i], i);

				type[i] = JavaTypes.getJavaType( columnDefinition2.getColDataType().getDataType() );
				List<String> columnSpecString = columnDefinition2.getColumnSpecStrings();
				//System.out.println(columnDefinition2.getColumnSpecStrings());
				if(columnSpecString != null)
					if(columnSpecString.get(0).equals("PRIMARY")) {
						columnIndex.add(columns[i]);
						stats.primary = columns[i];
						TableStats.primaryKeys.add(columns[i].getColumnName());
					} else {
						columnRef.add(columns[i]);
					}
				i++;
			}
			stats.columnRef = columnRef;

			schemaCollection.put(create.getTable().getName(), columns);
			schemaCollectionType.put(create.getTable().getName(), type);


			File table = new File( "data/" + tableName);
			table.mkdir();

			
			try {
				
				Long position = (long) 1000;
				
				RandomAccessFile fileO = new RandomAccessFile("data/" + tableName + "/" + stats.primary, "rw");
				
//				BufferedWriter bw = null;
//				FileOutputStream fileO = new FileOutputStream("data/" + tableName + "/" + stats.primary);
//				PrintWriter printWriter = new PrintWriter( new BufferedOutputStream(fileO) );
				
				FileInputStream fileI = new FileInputStream("data/" + tableName + ".dat");
				
				BufferedReader reader = new BufferedReader( new InputStreamReader(fileI) );
				CSVParser csv = new CSVParser( reader , CSVFormat.newFormat('|'));
				Iterator<CSVRecord> parser = csv.iterator();

				while( parser.hasNext() ) {

					CSVRecord csvRecord = parser.next();
					List<String> stringList = new ArrayList<>();
					
					for(int j = 0; j < csvRecord.size(); j++){
						stringList.add(csvRecord.get(j));
					}
					
					String csvString = String.join("|", stringList);
					
					record = new TupleRecord(csvRecord, type);
					
					/************************** For primary keys ***************************/
					if( !columnIndex.isEmpty() ) {

						for( Column column : columnIndex ) {
							PrimitiveValue colPrimitive = null;

								colPrimitive = record.getRecord( schemaWhole.get(column) );
//								position = fileI.getChannel().position();
								
//								fileO.getChannel().position(position);
//								printWriter.write(csvString);
//								printWriter.write("\n");
								
								position = fileO.getFilePointer();
//								position = colPrimitive.toLong() * 100;
//								fileO.seek(position);
								
//								FileWriter fw = new FileWriter(fileO.getFD());
//								bw = new BufferedWriter(fw);
//								bw.write(csvString);
//								bw.newLine();
//								bw.flush();
								
								fileO.writeUTF(csvString);
								
								stats.primaryIndex.put(colPrimitive, position);
//								stats.primarySet.add(position);
								System.err.println( colPrimitive.hashCode() + " the position for " + colPrimitive + " is " + position + " csv : " + csvString);
//								position += 10240;
						}
					}
					/************************** For primary keys ***************************/

					stats.totalCount++;
				}
				csv.close();
//				printWriter.close();
				fileO.close();

				statitics.put(tableName, stats);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
//			catch (InvalidPrimitive e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		else {
			System.out.println("Can not find the query");
			throw error;
		}

		System.err.println("Starting optimization...");

		if(node != null) {
			node.open(null);

			OptimizeProjection.optimize(node);

			node.open(null);


			Optimize.optimize(node, null, true);

			node.open(null);

		}

		System.err.println("Done");
//		printTree(node);

		return node;
	}

	public void printTree(TreeNode node){
		TreeNode root = node, right;
		root = node;
		System.err.println("\n After optimization in Planner ---- ");

		while(root != null ) {
			Class<?> enclosingClass = root.getClass().getEnclosingClass();
			if (enclosingClass != null) {
				System.err.println(enclosingClass.getName());
			} else {
				System.err.println(root.getClass().getName());
				if(root instanceof JoinProduct) {

					right = ((JoinProduct) root).getRight();
					while(right != null) {
						enclosingClass = right.getClass().getEnclosingClass();
						if (enclosingClass != null) {
							System.err.println("\t Right val --------  " + enclosingClass.getName());
						} else {
							System.err.println("\t Right val --------  " + right.getClass().getName());
						}
						right = right.getChild();
					}
				}
			}
			root = root.getChild();
		}
	}

	public int getQueryType() {
		return queryType;
	}

	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}
}