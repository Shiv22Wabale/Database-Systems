package runnableClasses;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.*;

import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.select.SubSelect;
import types.TupleRecord;

public class Selection implements TreeNode{

	TreeNode child;
	Column[] schema;
	Expression expression;

	TupleRecord tuple;

	public Selection(TreeNode child, Expression expression) {
		this.child = child;
		
		this.expression = expression;

	}

	@Override
	public void open(Column primary) {
		child.open(primary);
		this.schema = child.getNodeSchema();
		//System.err.println(" opening ... " + schema);
	}
	
	Eval eval = new Eval() {
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			int index = 0;
			//System.err.println(schema);
			for(Column column : schema) {

				//schemaWhole.put(column, index);
				if(column.equals(arg0))
					return tuple.getRecord(index);
				index++;
			}

			//return tuple[schemaWhole.get(arg0)];
			return null;
		}
	};
	
	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {
		// TODO Auto-generated method stub
		tuple = null;
		do {
			tuple = child.nextTuple(primary, condition, once);

			if(tuple == null)
				return null;

			try {
				if(expression == null)
					return tuple;
				//System.err.println(expression);
				if(!eval.eval(expression).toBool()) {
					tuple = null;
				}
			} catch (InvalidPrimitive e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
//			System.err.println(" the tuple is " + tuple + " the once " + once );

		}while(tuple == null && !once);
		
//		System.err.println(" returning " + tuple);

		return tuple;
	}

	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

	@Override
	public void restart() {
		child.restart();
	}

	abstract class EvalCust extends Eval{

		public PrimitiveValue eval(Expression e)
				throws SQLException
		{
			if(e instanceof Addition){ return eval((Addition)e); }
			else if(e instanceof Division){ return eval((Division)e); }
			else if(e instanceof Multiplication){ return eval((Multiplication)e); }
			else if(e instanceof Subtraction){ return eval((Subtraction)e); }
			else if(e instanceof AndExpression){ return eval((AndExpression)e); }
			else if(e instanceof OrExpression){ return eval((OrExpression)e); }
			else if(e instanceof EqualsTo){ return eval((EqualsTo)e); }
			else if(e instanceof NotEqualsTo){ return eval((NotEqualsTo)e); }
			else if(e instanceof GreaterThan){ return eval((GreaterThan)e); }
			else if(e instanceof GreaterThanEquals){ return eval((GreaterThanEquals)e); }
			else if(e instanceof MinorThan){ return eval((MinorThan)e); }
			else if(e instanceof MinorThanEquals){ return eval((MinorThanEquals)e); }
			else if(e instanceof DateValue){ return eval((DateValue)e); }
			else if(e instanceof DoubleValue){ return eval((DoubleValue)e); }
			else if(e instanceof LongValue){ return eval((LongValue)e); }
			else if(e instanceof StringValue){ return eval((StringValue)e); }
			else if(e instanceof TimestampValue){ return eval((TimestampValue)e); }
			else if(e instanceof TimeValue){ return eval((TimeValue)e); }
			else if(e instanceof CaseExpression){ return eval((CaseExpression)e); }
			else if(e instanceof Column){ return eval((Column)e); }
			else if(e instanceof WhenClause){ return eval((WhenClause)e); }
			else if(e instanceof AllComparisonExpression){ return eval((AllComparisonExpression)e); }
			else if(e instanceof AnyComparisonExpression){ return eval((AnyComparisonExpression)e); }
			else if(e instanceof Between){ return eval((Between)e); }
			else if(e instanceof ExistsExpression){ return eval((ExistsExpression)e); }
			else if(e instanceof InExpression){ return eval((InExpression)e); }
			else if(e instanceof LikeExpression){ return eval((LikeExpression)e); }
			else if(e instanceof Matches){ return eval((Matches)e); }
			else if(e instanceof BitwiseXor){ return eval((BitwiseXor)e); }
			else if(e instanceof BitwiseOr){ return eval((BitwiseOr)e); }
			else if(e instanceof BitwiseAnd){ return eval((BitwiseAnd)e); }
			else if(e instanceof Concat){ return eval((Concat)e); }
			else if(e instanceof Function){ return eval((Function)e); }
			else if(e instanceof InverseExpression){ return eval((InverseExpression)e); }
			else if(e instanceof IsNullExpression){ return eval((IsNullExpression)e); }
			else if(e instanceof JdbcParameter){ return eval((JdbcParameter)e); }
			else if(e instanceof NullValue){ return eval((NullValue)e); }
			else if(e instanceof SubSelect){ return eval((SubSelect)e); }
			throw new SQLException("Invalid operator: "+e);
		}

//		public PrimitiveValue eval(SubSelect a)
//				throws SQLException
//		{ 
//			try {
//				TreeNode query = new SubQuery(a);
//				if(query.hasNext()) {
//					TupleRecord primitiveValues = query.nextTuple();
//					//System.out.println(primitiveValues);
//					return primitiveValues.getRecord(0);
//				}
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			return new LongValue(99999999);
//		}

		public PrimitiveValue eval(InExpression in) throws SQLException
		{ 
			PrimitiveValue lhs = eval(in.getLeftExpression());
			ItemsList rhsList = in.getItemsList();
			for(Expression expression : ( (ExpressionList)rhsList ).getExpressions() ) {
				PrimitiveValue rhs = eval(expression);
				if(lhs.equals(rhs))
					return BooleanValue.TRUE;
			}
			return BooleanValue.FALSE;
		}

	}

	public void setExpression(Expression exression) {
		//System.out.println("Setting the expression");
		this.expression = exression;
	}

	public Expression getExpression() {
		return this.expression;
	}

	@Override
	public TreeNode getChild() {
		// TODO Auto-generated method stub
		return child;
	}

	@Override
	public void setChild(TreeNode node) {
		// TODO Auto-generated method stub
		this.child = node;
		//this.schema = child.getNodeSchema();
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return child.hasNext();
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
			evalCol.eval(expression);
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		return columns;
	}

	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		return child.getEstimate();
	}
	
}