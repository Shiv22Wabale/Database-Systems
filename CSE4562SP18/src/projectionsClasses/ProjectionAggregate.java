package projectionsClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import runnableClasses.TreeNode;
import types.TupleRecord;

public class ProjectionAggregate extends ProjectionBase{
	
	boolean firstTime = true;
	int index = 0;

	public ProjectionAggregate(TreeNode child, List<SelectItem> items ) {
		super(child, items);

		firstTime = true;
		index = 0;
	}

	HashMap<List<PrimitiveValue>, List<PrimitiveValue> > aggMap = new HashMap<>();
	
	HashMap<List<PrimitiveValue>,  Integer> aggCount = new HashMap<>();
	
	Iterator<List<PrimitiveValue>> keyIterator = null;


	TupleRecord tuple = null;

	Eval eval = new Eval() {

		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			return tuple.getRecord( schemaWhole.get(arg0) );
		}
	};

	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {

		try {

			if(firstTime) {

				List<PrimitiveValue> groubyValues = new ArrayList<>();
				groubyValues.add(new LongValue(0));
				
				List<PrimitiveValue> output;
				HashSet<List<PrimitiveValue>> keys = new HashSet<>(); 

//				int identifier = 0;
				while( ( tuple = child.nextTuple(primary, condition, once) ) != null ) {
					
					output = new ArrayList<>();

					PrimitiveValue current = null;
					int count = 0;
					List<PrimitiveValue> currentWhole = null;
					Iterator<PrimitiveValue> iterator = null;

					if( aggMap.containsKey(groubyValues) ) {
						//stored.addAll( aggMap.get(groubyValues) );

						iterator = aggMap.get(groubyValues).iterator();
						count = aggCount.get(groubyValues);
					}

					for (SelectItem item : this.items) {

						if( iterator != null ) {
							current = iterator.next();
						}
						if(item instanceof AllColumns) {
							//tuple = child.nextTuple();
							//							if(tuple == null)
							//								return null;
							output.addAll( tuple.getAll() );
						}
						else if(item instanceof AllTableColumns) {
							//							tuple = child.nextTuple();
							//							if(tuple == null)
							//								return null;
							AllTableColumns allTableColumns = (AllTableColumns)item;
							int index = 0;
							for(Column column : schema) {

								if(column.getTable().getName().equals(allTableColumns.getTable().getName())) {
									output.add( tuple.getRecord(index) );
								}
								index++;
							}
						}
						else if(item instanceof SelectExpressionItem) {
							SelectExpressionItem expressionItem = (SelectExpressionItem)item;
							Expression expression = expressionItem.getExpression();
							if( expression instanceof Function){
								Function function = (Function)expression;

								if(function.isAllColumns()){
									//System.out.println(current + "comparing with " + tuple.getAll() );

									if(function.getName().toUpperCase().equals("COUNT")) {
										try {
											if(current == null)
												expression = (new CCJSqlParser(new StringReader("1"))).Expression();
											else
												expression = (new CCJSqlParser(new StringReader( current + " + 1"))).Expression();
										} catch (ParseException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}

										output.add(eval.eval(expression));
									}
									else {
										if(current != null) {
											currentWhole = new ArrayList<>();
											currentWhole.add(current);
											for(int i = 0; i < tuple.size() - 1; i++)
												currentWhole.add( iterator.next() );
										}

										List<PrimitiveValue> tupleAggs = Aggregate.solveAll(function.getName().toUpperCase(), currentWhole, tuple, count);
										output.addAll(tupleAggs);
									}
									//System.out.println(output);
								}
								else {
									for (Expression aggregate : function.getParameters().getExpressions()) {
										//System.out.println(function.getName().toUpperCase() + " for " + aggregate);
										PrimitiveValue tupleAgg = Aggregate.solve(function.getName().toUpperCase(), current, eval.eval(aggregate), count);
										if(tupleAgg == null)
											return null;
										output.add(tupleAgg);
									}
								}
							}
							else {
								output.add( eval.eval(expression) );
							}

						}
					}

					//TupleRecord record = new TupleRecord(groubyValues);
					//System.out.println(groubyValues.hashCode() + " + " + groubyValues + " contains === " + aggMap.containsKey(groubyValues) );
					aggMap.put(groubyValues, output);
					aggCount.put(groubyValues, count + 1);

					//System.out.println(record.hashCode() + " + " + record.getAll() + " contains === " + aggMap.containsKey(record) );
					//Collections.sort(keys, new PrimitiveListComparator());
					keys.add(groubyValues);
				}

				//List<List<PrimitiveValue>> lists = new ArrayList<List<PrimitiveValue>>(keys);

				//Collections.sort(lists, new PrimitiveListComparator());

				//keyIterator = lists.iterator();
				keyIterator = keys.iterator();
				firstTime = false;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(keyIterator.hasNext()) {
			return new TupleRecord( aggMap.get(keyIterator.next()) );
		}

		return null;
		//return child.nextTuple();
	}

	@Override
	public void restart() {
		// TODO Auto-generated method stub
		firstTime = true;
		index = 0;
	}

}
