package runnableClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.OrderByElement;
import types.TupleRecord;

public class PrimitiveValueComparator implements Comparator<TupleRecord> {

	List<OrderByElement>  orderByElementsList;
	Column[] schema;

	TupleRecord o;

	public PrimitiveValueComparator(List<OrderByElement> orderByElementsList, Column[] schema) {
		this.orderByElementsList = orderByElementsList;
		this.schema = schema;
	}

	@Override
	public int compare(TupleRecord o1, TupleRecord o2) {


		for(OrderByElement byElement : orderByElementsList) {

			int asc = byElement.isAsc() ? 1 : -1;


			Eval eval1 = new Eval() {

				@Override
				public PrimitiveValue eval(Column arg0) throws SQLException {

					int index = 0;
					//System.err.println(schema);
					for(Column column : schema) {

						//schemaWhole.put(column, index);
						if(column.equals(arg0))
							return o.getRecord(index);
						index++;
					}

					//return tuple[schemaWhole.get(arg0)];
					return null;
					//					// TODO Auto-generated method stub
					//					PrimitiveValue value = new LongValue(111111);
					//
					//					int index = 0;
					//					if( arg0.getTable().getName() == null ) {
					//
					//						for(Column column : schema) {
					//
					//							if(arg0.getColumnName().compareTo(column.getColumnName()) == 0) {
					//								return o.getRecord(index);
					//							}
					//							index++;
					//						}
					//					}
					//					else {
					//						for(Column column : schema) {
					//							if(arg0.getWholeColumnName().compareTo(column.getWholeColumnName()) == 0) {
					//								return o.getRecord(index);
					//							}
					//							index++;
					//						}
					//					}
					//					//finalIndex = index;
					//					return value;
				}
			};

			Eval eval = new Eval() {

				@Override
				public PrimitiveType escalateNumeric(PrimitiveType lhs, PrimitiveType rhs)
						throws SQLException
				{
					if(lhs == PrimitiveType.DATE) {
						if(rhs == PrimitiveType.DATE) { return PrimitiveType.DATE; }
					}
					if(lhs == PrimitiveType.STRING) {
						if(rhs == PrimitiveType.STRING) { return PrimitiveType.STRING; }
					}
					if(  (assertNumeric(lhs) == PrimitiveType.DOUBLE)
							||(assertNumeric(rhs) == PrimitiveType.DOUBLE)){
						return PrimitiveType.DOUBLE;
					} else {
						return PrimitiveType.LONG;
					}
				}

				@Override
				public PrimitiveValue cmp(BinaryExpression e, CmpOp op)
						throws SQLException
				{
					try {
						PrimitiveValue lhs = eval(e.getLeftExpression());
						PrimitiveValue rhs = eval(e.getRightExpression());
						if(lhs == null || rhs == null) return null;
						boolean ret;

						switch(escalateNumeric(getPrimitiveType(lhs), getPrimitiveType(rhs))){
						case DOUBLE:
							ret = op.op(lhs.toDouble(), rhs.toDouble());
							break;
						case LONG:
							ret = op.op(lhs.toLong(), rhs.toLong());
							break;
						case STRING:
							//System.out.println("Comparing " + lhs.toString() + " with " + rhs.toString());
							ret = lhs.toString().compareTo(rhs.toString()) > 0;
							break;
						case DATE: {
							DateValue dlhs = (DateValue)lhs,
									drhs = (DateValue)rhs;
							ret = op.op(
									dlhs.getYear()*10000+
									dlhs.getMonth()*100+
									dlhs.getDate(),
									drhs.getYear()*10000+
									drhs.getMonth()*100+
									drhs.getDate()
									);
						}
						break;
						default: 
							throw new SQLException("Invalid PrimitiveType escalation");
						}
						return ret ? BooleanValue.TRUE : BooleanValue.FALSE;
					} catch(PrimitiveValue.InvalidPrimitive ex) { 
						throw new SQLException("Invalid leaf value", ex);
					}
				}

				public PrimitiveValue eval(LongValue v) { 
					try {
						if(v.getValue() == 1) {
							//System.out.println("It's o1");
							o = o1;
							return eval1.eval(byElement.getExpression());
						}
						else {
							//System.out.println("It's o2");
							o = o2;
							return eval1.eval(byElement.getExpression());
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return v;
				}

				@Override
				public PrimitiveValue eval(Column arg0) throws SQLException {
					// TODO Auto-generated method stub
					if(arg0.equals("A")) {
						o = o1;
						return eval1.eval(byElement.getExpression());
					}
					else {
						o = o2;
						return eval1.eval(byElement.getExpression());
					}
				}
			};

			try {

				//				Expression expressionGreater = (new CCJSqlParser(new StringReader("A > B"))).Expression();
				GreaterThan expressionGreater = new GreaterThan();
				expressionGreater.setLeftExpression(new LongValue(1));
				expressionGreater.setRightExpression(new LongValue(2));

				//				Expression expressionLess = (new CCJSqlParser(new StringReader("B > A"))).Expression();
				GreaterThan expressionLess = new GreaterThan();
				expressionLess.setLeftExpression(new LongValue(2));
				expressionLess.setRightExpression(new LongValue(1));

				if(eval.eval(expressionGreater).toBool())
					return 1 * asc;
				else if(eval.eval(expressionLess).toBool())
					return -1 * asc;

			} catch ( SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			//			catch (ParseException e1) {
			//				// TODO Auto-generated catch block
			//				e1.printStackTrace();
			//			}

		}

		return 0;
	}



}