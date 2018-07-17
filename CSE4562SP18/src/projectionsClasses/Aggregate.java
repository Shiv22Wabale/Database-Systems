package projectionsClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import runnableClasses.TreeNode;
import types.TupleRecord;

public class Aggregate {

	TreeNode child;
	HashMap<Column, Integer> schemaWhole = new HashMap<>();
	List<Column> groupByList;

	public Aggregate(TreeNode child, HashMap<Column, Integer> schemaWhole, List<Column> groupByList) {

		this.child = child;
		this.schemaWhole = schemaWhole;
		this.groupByList = groupByList;
	}

	TupleRecord tuple = null;

	public Eval eval = new Eval() {
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			return tuple.getRecord( schemaWhole.get(arg0) );
		}
	};

	static Expression expression = null;
	public static List<PrimitiveValue> solveAll(String functionName, List<PrimitiveValue> currentWhole, TupleRecord updates, int count) throws InvalidPrimitive, SQLException {


		//		try {

		List<PrimitiveValue> output = new ArrayList<>();

		switch (functionName) {

		case "SUM":
			//double sum = current != null ? current.toDouble() : 0;

			//return new DoubleValue(sum + Update.toDouble());
			if(currentWhole == null)
				return updates.getAll();
			else {

				Addition addition = new Addition();
				int i = 0;
				for(PrimitiveValue current : currentWhole) {
					addition.setLeftExpression(current);
					addition.setRightExpression(updates.getRecord(i++));
					output.add(evalE.eval(addition));
				}
				//System.out.println(output);
				return output;
			}



		case "MIN":
			if(currentWhole == null) {
				return updates.getAll();
			} else {
				GreaterThan greater = new GreaterThan();
				int i = 0;
				for(PrimitiveValue current : currentWhole) {
					PrimitiveValue update = updates.getRecord(i++);
					greater.setLeftExpression(current);
					greater.setRightExpression(update);
					if(evalE.eval(greater).toBool())
						output.add(update);
					else
						output.add(current);
				}
				//					expression = (new CCJSqlParser(new StringReader( currentWhole + " < " ))).Expression();
				//					if(evalE.eval(expression).toBool()) {
				//						output.addAll(currentWhole);
				//						return output;
				//					}
				//return update;
				return output;
			}

		case "MAX":
			if(currentWhole == null) {
				return updates.getAll();
			} else {
				GreaterThan greater = new GreaterThan();
				int i = 0;
				for(PrimitiveValue current : currentWhole) {
					PrimitiveValue update = updates.getRecord(i++);
					greater.setLeftExpression(current);
					greater.setRightExpression(update);
					if(evalE.eval(greater).toBool())
						output.add(current);
					else
						output.add(update);
				}
				return output;
			}

		case "AVG":
			if(currentWhole == null) {
				currentWhole = new ArrayList<>();
				for(int i = 0; i < updates.size(); ++i )
					currentWhole.add(new DoubleValue(0));
			}

			Division division = new Division();
			//PrimitiveValue c = new LongValue(count);
			division.setRightExpression(new DoubleValue(count + 1));

			Addition addition = new Addition();
			int i = 0;
			for(PrimitiveValue current : currentWhole) {
				addition.setLeftExpression(new Multiplication(current, new DoubleValue(count)));
				addition.setRightExpression(updates.getRecord(i++));

				division.setLeftExpression(addition);
				output.add(evalE.eval(division));
			}
			return output;


		default:
			break;
		}


		//		} catch (ParseException | SQLException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}


		return null;

	}

	public  static Eval evalE = new Eval() {
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			System.err.println(arg0);
			return null;
		}
	};

	public static PrimitiveValue solve(String functionName, PrimitiveValue current, PrimitiveValue update, int count) throws InvalidPrimitive, SQLException {

		try {

			switch (functionName) {

			case "COUNT":
				//int count = current != null ? current.toDouble() + 1: 1;
				//return new DoubleValue(count);
				//System.out.println(update);
				if(current == null)
					//expression = (new CCJSqlParser(new StringReader("1"))).Expression();
					return new LongValue(1);
				else {
					Addition addition = new Addition();
					addition.setLeftExpression(current);
					addition.setRightExpression(new LongValue(1));
					return evalE.eval(addition);
				}
				//System.out.println(output);
				//return evalE.eval(expression);

			case "SUM":
				//double sum = current != null ? current.toDouble() : 0;

				//return new DoubleValue(sum + Update.toDouble());
				if(current == null)
					return update;
				else {
					Addition addition = new Addition();

					addition.setLeftExpression(current);
					addition.setRightExpression(update);
//					PrimitiveValue temp = evalE.eval(addition);
//					System.err.println(temp);
					return evalE.eval(addition);
				}
			case "MIN":
				if(current == null) {
					return update;
				} else {
					GreaterThan greater = new GreaterThan();
					greater.setLeftExpression(current);
					greater.setRightExpression(update);
					if(evalE.eval(greater).toBool())
						return current;
					else
						return update;

				}

			case "MAX":
				if(current == null) {
					return update;
				} else {
					GreaterThan greater = new GreaterThan();
					greater.setLeftExpression(current);
					greater.setRightExpression(update);
					if(evalE.eval(greater).toBool())
						return update;
					else
						return current;
				}

			case "AVG":
				if(current == null) {
					current = new DoubleValue(0);
				}
				Division division = new Division();

				division.setRightExpression(new DoubleValue(count + 1));

				Addition addition = new Addition();

				addition.setLeftExpression(new Multiplication(current, new DoubleValue(count)));
				addition.setRightExpression(update);

				division.setLeftExpression(addition);
				return evalE.eval(division);
				//output.add(evalE.eval(division));
				//return output;


			default:
				break;
			}


		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return null;

	}
}