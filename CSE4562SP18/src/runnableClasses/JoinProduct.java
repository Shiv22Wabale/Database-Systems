package runnableClasses;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.www.cse4562.ExecuteFromItem;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import types.TableStats;
import types.TupleRecord;

public class JoinProduct implements TreeNode{


	TreeNode left;
	TreeNode right;
	Join join;
	Column[] schema;

	Expression expression;

	boolean firstTime = true;

	boolean end = false;

	HashMap<Column, Integer> schemaWhole;

	public JoinProduct(TreeNode left, Join join) {

		firstTime = true;

		end = false;

		this.left = left;
		this.join = join;

		ExecuteFromItem executeFromItem = new ExecuteFromItem();
		join.getRightItem().accept(executeFromItem);
		this.right = executeFromItem.getNode();

		expression = join.getOnExpression();

		setSchema();

	}

	boolean index = false;
	EqualsTo equalsTo;
	Column leftCol;
	Column rightCol;

	@Override
	public void open(Column primary) {
		left.open(null);
		right.open(null);

		setSchema();

		if(expression instanceof EqualsTo) {

			equalsTo = (EqualsTo)expression;
			//System.err.println(equalsTo);
			Column column = (Column)equalsTo.getLeftExpression();

			if( schemaWhole.get(column) >= leftColLength ) {
				Expression temp = equalsTo.getRightExpression();
				equalsTo.setLeftExpression(temp);
				equalsTo.setRightExpression(column);
				System.err.println("Changes performed " + equalsTo);
			}

			leftCol = (Column)equalsTo.getLeftExpression();
			rightCol = (Column)equalsTo.getRightExpression();

			//			System.err.println( column + " the changes with schema value : " + schemaWhole.get(column) + " the len is : " + leftColLength);
			System.err.println(" The left exp is : " + leftCol + " the right expression is " + rightCol);
			if(TableStats.primaryKeys.contains(leftCol.getColumnName())) {
				System.err.println(" The left side matched with primary key " + leftCol);

				TreeNode temp = left;
				left = right;
				right = temp;
				this.open(primary);

//				index = true;
			}
			else if(TableStats.primaryKeys.contains(rightCol.getColumnName())) {
				System.err.println(" The right side matched with primary key " + rightCol);
//				index = true;
			}
			
			index = ((Column)equalsTo.getLeftExpression()).equals(new Column(null, "L_PARTKEY")) && ((Column)equalsTo.getRightExpression()).equals(new Column(null, "P_PARTKEY"));//.equals(new EqualsTo(new Column(null, "L_PARTKEY"), new Column(null, "L_PARTKEY")));
//			index = equalsTo.equals(new EqualsTo(new Column(null, "L_PARTKEY"), new Column(null, "P_PARTKEY")));
			System.err.println(" the comparision result is " + ((Column)equalsTo.getLeftExpression()).equals(new Column(null, "L_PARTKEY")) );
			System.err.println(" the index is " + index + " the join is : " + equalsTo);
		}
	}

	int leftColLength = 0;
	void setSchema() {

		schemaWhole = new HashMap<>();

		Column[] leftCol = left.getNodeSchema();
		Column[] rightCol = right.getNodeSchema();

		leftColLength = leftCol.length;
		schema = new Column[leftCol.length + rightCol.length];


		int i = 0;
		for(Column column : leftCol)
			schema[i++] = column;
		for(Column column : rightCol)
			schema[i++] = column;

		int index = 0;
		for(Column column : schema) {
			schemaWhole.put(column, index);
			index++;
		}

	}

	Eval eval = new Eval() {

		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			int index = schemaWhole.get(arg0);
			return index < leftColLength ? leftTuple.getRecord(index) : rightTuple.getRecord(index - leftColLength);

		}
	};

	//static


	TupleRecord leftTuple = null;
	TupleRecord rightTuple = null;

	//	@Override
	//	public boolean hasNext() {
	//		if(end) {
	//			return false;
	//		}
	//
	//		if(firstTime) {
	//
	//			firstTime = false;
	//			if(left.hasNext()) {
	//				leftTuple = left.nextTuple();
	//				System.out.println("Setting 1" +leftTuple);
	//				if(right.hasNext())
	//					return true;
	//			}
	//
	//		}
	//
	//		if(right.hasNext())
	//			return true;
	//
	//		
	//		if(left.hasNext()) {
	//			System.out.println("Resting right");
	//			
	//			right.restart();
	//
	//			leftTuple = left.nextTuple();
	//			System.out.println("Setting 2" + leftTuple);
	//			return true;
	//		}
	//
	//		end = true;
	//		return false;
	//	}

	/********************************* Logic for nested loop join ************************/
	//		@Override
	//		public TupleRecord nextTuple() {
	//	
	//			TupleRecord tuple = null;
	//	
	//			//		if(leftTuple == null)
	//			//			return null;
	//	
	//			do {
	//	
	//				if( ( rightTuple = right.nextTuple() ) == null ) {
	//					if( ( leftTuple = left.nextTuple() ) == null)
	//						return null;
	//	
	//					right.restart();
	//					rightTuple = right.nextTuple();
	//					//				if(rightTuple == null)
	//					//					System.out.println("There is problem");
	//				}
	//	
	//				if(leftTuple == null)
	//					if( ( leftTuple = left.nextTuple() ) == null)
	//						return null;
	//	
	//				if(expression != null ) {
	//					try {
	//						if( eval.eval(expression).toBool() ) {
	//							tuple = createTuple();
	//						}
	//						//					else {
	//						//						tuple = createTuple();
	//						//					}
	//					} catch (SQLException e) {
	//						// TODO Auto-generated catch block
	//						e.printStackTrace();
	//					}
	//				}
	//				else {
	//					tuple = createTuple();
	//				}
	//	
	//			} while(tuple == null);
	//	
	//			//System.out.println("Retrunging from join " + tuple);
	//			return tuple;
	//		}
	/********************************* Logic for nested loop join ************************/

	/**************** Logic for everything in memory *************/

	//	int block = 5000;
	int size = 0;


	Expression rightKey;
	PrimitiveValue keyR;
	//TupleRecord[] rightArray = new TupleRecord[5000];
	HashMap<PrimitiveValue, List<TupleRecord>> rightMap = new HashMap<>();
	void loadRight() {
		try {
			size = 0;
			List<TupleRecord> records = null;
			while( (rightTuple = right.nextTuple(null, null, false) ) != null) {

				PrimitiveValue key = eval.eval(rightKey);
				if(rightMap.containsKey(key))
					records = rightMap.get(key);
				else
					records = new ArrayList<>();

				records.add(rightTuple);
				rightMap.put(key, records);

				//rightArray[size++] = rightTuple;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//rightTuple = null;
	}

	Expression leftKey;
	PrimitiveValue keyL;
	//TupleRecord[] leftArray = new TupleRecord[5000];
	HashMap<PrimitiveValue, List<TupleRecord>> leftMap = new HashMap<>();
	Iterator<PrimitiveValue> leftKeySet;
	void loadLeft() {

		try {
			size = 0;
			List<TupleRecord> records = null;
			while( (leftTuple = left.nextTuple(null, null, false) ) != null) {

				PrimitiveValue key = eval.eval(leftKey);
				if(leftMap.containsKey(key))
					records = leftMap.get(key);
				else
					records = new ArrayList<>();

				records.add(leftTuple);
				leftMap.put(key, records);

				//rightArray[size++] = rightTuple;
			}
			//System.err.println(leftMap.keySet());
			leftKeySet = leftMap.keySet().iterator();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(size);
		//leftTuple = null;
	}

	int countLeft = 0;
//	int count = 0;
	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {

		/**************** Logic for everything in memory *************/
		if( !index ) {
			if(firstTime) {
				firstTime = false;

				//System.err.println(expression);
				if(expression instanceof EqualsTo) {

					//System.err.println("It's equality join....");
					//System.out.println();
					Eval evalFind = new Eval() {
						@Override
						public PrimitiveValue eval(Column arg0) throws SQLException {
							// TODO Auto-generated method stub
							//System.err.println("Checking for " + arg0);
							int index = schemaWhole.get(arg0);
							//System.err.println("Checking for " + arg0 + " with index " + index);
							if( index < leftColLength ) {
								leftKey = arg0;
								//System.err.println("left key is ------- " + leftKey);
							} else {
								rightKey = arg0;
								//System.err.println("right key is ------- "+ rightKey);
							}
							return null;
						}
					};

					try{
						evalFind.eval(expression);
					}
					catch (Exception e) {
						// TODO: handle exception
					}
				}
				else {
					leftKey = rightKey = new LongValue(0);
				}

				//long startTime = System.currentTimeMillis();
				loadRight();

				//			long endTime = System.currentTimeMillis();
				//			System.err.println("Loading time for right " + (endTime - startTime));

				//startTime = System.currentTimeMillis();
//				loadLeft();

				//			endTime = System.currentTimeMillis();
				//			System.err.println("Loading time for left " + (endTime - startTime));
			}

			TupleRecord tuple = null;


			do {

				if(leftTuple == null) {
					if( ( leftTuple = left.nextTuple(null, null, false)) == null)
						return null;
					try {
						keyL = eval.eval(leftKey);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					if(!leftKeySet.hasNext())
//						return null;
//					keyL = leftKeySet.next();
//					if( ( leftTuple = getLeftTuple(keyL)) == null)
//						return null;
				}

				if( ( rightTuple = getRightTuple(keyL, false) ) == null ) {

					do {
						if( ( leftTuple = left.nextTuple(null, null, false)) == null)
							return null;
						try {
							keyL = eval.eval(leftKey);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
//						count = 0;
						rightTuple = getRightTuple(keyL, true);
					}
					while( rightTuple == null );
				}

				//tuple = createTuple();

//				if(expression == null || expression instanceof EqualsTo) {
					//System.err.println("It's equality join....");
					tuple = createTuple();
//				} else {
//					try {
//						if( eval.eval(expression).toBool() ) {
//							tuple = createTuple();
//						}
//					} catch (SQLException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}


			} while(tuple == null);

			return tuple;

			/**************** Logic for everything in memory *************/
		}
		else {
			TupleRecord tuple = null;

			//		if(leftTuple == null)
			//			return null;

			do {

//				if(leftTuple == null)
				if( ( leftTuple = left.nextTuple(primary, condition, once) ) == null)
					return null;

				PrimitiveValue cond = leftTuple.getRecord( schemaWhole.get( leftCol ));

//				System.err.println( " the join is " + equalsTo + " the ---->  " + leftTuple.getAll() + " Asking for " + cond);

				//					EqualsTo equalsTo = (EqualsTo)expression, cond = new EqualsTo();
				//					//System.err.println(equalsTo);
				//					Column column = (Column)equalsTo.getLeftExpression();
				//
				//					cond.setRightExpression( (Column)equalsTo.getLeftExpression() );
				//					cond.setLeftExpression(equalsTo.getRightExpression());

				if ( ( rightTuple = right.nextTuple(rightCol, cond, true) ) != null )  {
					
//					System.err.println("right is not null " + rightTuple);
					
//					if( ( leftTuple = left.nextTuple(primary, condition) ) == null)
//						return null;

					//						cond = new EqualsTo();
					//
					//						cond.setRightExpression( leftTuple.getRecord(schemaWhole.get(column)) );
					//						cond.setLeftExpression(equalsTo.getRightExpression());

//					cond = leftTuple.getRecord(schemaWhole.get( leftCol ));

					tuple = createTuple();
					//						right.restart();
					//rightTuple = right.nextTuple(cond);

					//				if(rightTuple == null)
					//					System.out.println("There is problem");
				}

//									try {
//										if( eval.eval(expression).toBool() ) {
//											tuple = createTuple();
//										}
//										//					else {
//										//						tuple = createTuple();
//										//					}
//									} catch (SQLException e) {
//										// TODO Auto-generated catch block
//										e.printStackTrace();
//									}
				

//				if(primary != null) {
//					int index = schemaWhole.get(primary);
//
//					//						System.err.println(tuple.getRecord(index) + " compare with " + cond + " the ans is : " + tuple.getRecord(index).equals(cond));
//					//						System.err.println(Arrays.asList(schema) + " " + tuple.getAll() + " --- " + primary + " --- index --- > " + index + " --- "+ tuple.getRecord(index) + " compare with " + condition + " the ans is : " + tuple.getRecord(index).equals(condition));
//					if(tuple.getRecord(index).equals(condition)) {
//						System.err.println(tuple.getAll() + " --- " + primary + " --- " + tuple.getRecord(index) + " compare with " + cond + " the ans is : " + tuple.getRecord(index).equals(cond));
////						tuple = null;
////						return null;
//					}
//				}
				
//				System.err.println( equalsTo + " --- " + tuple);
				
			} while(tuple == null && !once);

//			System.err.println("Retrunging from join " + tuple.getAll());
			return tuple;
		}
	}

	PrimitiveValue askedKeyR = null;
	Iterator<TupleRecord> rightRecords;
	TupleRecord getRightTuple(PrimitiveValue key, boolean restart) {
		//System.err.println(" asked right = " + key + "previous one " + askedKeyR + "is it restart " + restart );
		if( (askedKeyR == null || askedKeyR != key) || restart){
			//if(rightMap.get(key))
			if(!rightMap.containsKey(key))
				return null;

			rightRecords = rightMap.get(key).iterator();
			askedKeyR = key;
		}
		if(rightRecords.hasNext())
			return rightRecords.next();
		return null;
	}

	PrimitiveValue askedKeyL = null;
	Iterator<TupleRecord> leftRecords;
	TupleRecord getLeftTuple(PrimitiveValue key) {
		//System.err.println(" asked left = " + key + "previous one " + askedKeyL );
		if(askedKeyL == null || askedKeyL != key){
			leftRecords = leftMap.get(key).iterator();
			//System.err.println(" has --- " + leftRecords.hasNext());
			askedKeyL = key;
		}
		//System.err.println("Iterator : === "  + leftMap.get(key).get(0).getAll());
		if(leftRecords.hasNext())
			return leftRecords.next();
		return null;
	}

	/**************** Logic for everything in memory *************/


	/*************************** Logic for index loop join ***********/

	//	@Override
	//	public TupleRecord nextTuple(Expression condition) {
	//
	//		TupleRecord tuple = null;
	//
	//		//		if(leftTuple == null)
	//		//			return null;
	//
	//		do {
	//			
	//			if(leftTuple == null)
	//				if( ( leftTuple = left.nextTuple(null) ) == null)
	//					return null;
	//
	//			if(expression instanceof EqualsTo) {
	//				
	//				EqualsTo equalsTo = (EqualsTo)expression, cond = new EqualsTo();
	//				//System.err.println(equalsTo);
	//				Column column = (Column)equalsTo.getLeftExpression();
	//				
	//				cond.setRightExpression( leftTuple.getRecord(schemaWhole.get(column)) );
	//				cond.setLeftExpression(equalsTo.getRightExpression());
	//				
	//				while( ( rightTuple = right.nextTuple(cond) ) == null ) {
	//					if( ( leftTuple = left.nextTuple(null) ) == null)
	//						return null;
	//					
	//					cond = new EqualsTo();
	//					
	//					cond.setRightExpression( leftTuple.getRecord(schemaWhole.get(column)) );
	//					cond.setLeftExpression(equalsTo.getRightExpression());
	//					
	//					right.restart();
	//					//rightTuple = right.nextTuple(cond);
	//					
	//					//				if(rightTuple == null)
	//					//					System.out.println("There is problem");
	//				}
	//				
	//				try {
	//					if( eval.eval(expression).toBool() ) {
	//						tuple = createTuple();
	//					}
	//					//					else {
	//					//						tuple = createTuple();
	//					//					}
	//				} catch (SQLException e) {
	//					// TODO Auto-generated catch block
	//					e.printStackTrace();
	//				}
	//				
	////				tuple = createTuple();
	//			}
	//			else {
	//				if( ( rightTuple = right.nextTuple(null) ) == null ) {
	//					if( ( leftTuple = left.nextTuple(null) ) == null)
	//						return null;
	//	
	//					right.restart();
	//					rightTuple = right.nextTuple(null);
	//					
	//					//				if(rightTuple == null)
	//					//					System.out.println("There is problem");
	//				}
	//				
	//				if(expression != null ) {
	//					try {
	//						if( eval.eval(expression).toBool() ) {
	//							tuple = createTuple();
	//						}
	//						//					else {
	//						//						tuple = createTuple();
	//						//					}
	//					} catch (SQLException e) {
	//						// TODO Auto-generated catch block
	//						e.printStackTrace();
	//					}
	//				}
	//				else {
	//					tuple = createTuple();
	//				}
	//			}
	//			
	//			
	//
	//		} while(tuple == null);
	//
	//		//System.out.println("Retrunging from join " + tuple);
	//		return tuple;
	//	}

	/*************************** Logic for index loop join ***********/


	public TupleRecord createTuple() {
		int size = leftTuple.size() + rightTuple.size();


		TupleRecord tuple = new TupleRecord(size);


		tuple.addAll(leftTuple.getAll());
		tuple.addAll(rightTuple.getAll());

		return tuple;
	}


	public void setJoinExpression(Expression expression) {
		//join.setOnExpression(expression);
		this.expression = expression;
	}

	public Expression getJoinExpression() {
		//return join.getOnExpression();
		return expression;
	}

	@Override
	public Column[] getNodeSchema() {
		return schema;
	}


	@Override
	public void restart() {
		// TODO Auto-generated method stub
		end = false;
		firstTime = true;
		left.restart();
		System.err.println("Resting right");
		right.restart();
	}

	@Override
	public TreeNode getChild() {
		// TODO Auto-generated method stub
		return left;
	}

	public TreeNode getRight() {
		// TODO Auto-generated method stub
		return right;
	}

	@Override
	public void setChild(TreeNode node) {
		// TODO Auto-generated method stub
		this.left = node;
	}

	public void setRight(TreeNode node) {
		// TODO Auto-generated method stub
		this.right = node;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return left.hasNext();
	}

	@Override
	public List<Column> getColumn() {

		List<Column> columns = new ArrayList<>();
		// TODO Auto-generated method stub
		Eval evalCol = new Eval() {
			@Override
			public PrimitiveValue eval(Column arg0) throws SQLException {
				// TODO Auto-generated method stub
				columns.add(arg0);
				return null;
			}
		};

		try{
			evalCol.eval(join.getOnExpression());
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		return columns;
	}

	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		return left.getEstimate() * right.getEstimate();
	}


}