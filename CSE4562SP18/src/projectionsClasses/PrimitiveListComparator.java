package projectionsClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class PrimitiveListComparator implements Comparator<List<PrimitiveValue>> {

	int index = 0;
	
	@Override
	public int compare(List<PrimitiveValue> o1, List<PrimitiveValue> o2) {
		// TODO Auto-generated method stub
		
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

			@Override
			public PrimitiveValue eval(Column arg0) throws SQLException {
				// TODO Auto-generated method stub
				if(arg0.getColumnName().compareTo("A") == 0) {
					return o1.get(index);
				}
				else {
					return o2.get(index);
				}
			}
		};
		
		for( index = 0; index < o1.size(); index++ ) {
			try {
				Expression expressionGreater = (new CCJSqlParser(new StringReader("A > B"))).Expression();
				Expression expressionLess = (new CCJSqlParser(new StringReader("B > A"))).Expression();
				
				if(eval.eval(expressionGreater).toBool())
					return 1;
				else if(eval.eval(expressionLess).toBool())
					return -1;
				
			} catch (ParseException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}


}
