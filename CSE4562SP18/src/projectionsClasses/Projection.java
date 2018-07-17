package projectionsClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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

public class Projection extends ProjectionBase {

	public Projection(TreeNode child, List<SelectItem> items){

		super(child, items);

	}

	TupleRecord tuple = null;

	Eval eval = new Eval() {

		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			//			System.err.println("Asking " + arg0);
			//			System.err.println("Should have " + schemaWhole.keySet());

			return tuple.getRecord( schemaWhole.get(arg0) );
		}
	};

	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {

		List<PrimitiveValue> output = new ArrayList<>();


		tuple = child.nextTuple(primary, condition, once);
		if(tuple == null)
			return null;

		for (SelectItem item : this.items) {
			if(item instanceof AllColumns) {
				output.addAll( tuple.getAll() );
			}
			else if(item instanceof AllTableColumns) {
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

					for (Expression aggregate : function.getParameters().getExpressions()) {
						PrimitiveValue tupleAgg;
						try {
							tupleAgg = Aggregate.solve(function.getName().toUpperCase(), null, eval.eval(aggregate), 0);
							if(tupleAgg == null)
								return null;
							output.add(tupleAgg);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				else {
					try {
						output.add( eval.eval(expression) );
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}


			}
		}

		return new TupleRecord(output);
	}

	public void setItems(List<SelectItem> items){
		this.items = items;
	}

	@Override
	public void restart() {
		child.restart();
	}


}