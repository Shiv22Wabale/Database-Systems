package projectionsClasses;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.buffalo.www.cse4562.QuerySolver;
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
import net.sf.jsqlparser.statement.select.SubSelect;
import runnableClasses.TreeNode;

public abstract class ProjectionBase implements TreeNode{

	TreeNode child;
	List<SelectItem> items;

	Column[] schema;
	HashMap<Column, Integer> schemaWhole = new HashMap<>();



	public ProjectionBase(TreeNode child, List<SelectItem> items) {
		this.child = child;
		this.items = items;
	}
	
	@Override
	public void open(Column primary) {
		
		child.open(primary);
		
		this.schema = child.getNodeSchema();

		int index = 0;
		for(Column column : this.schema) {
			schemaWhole.put(column, index);
			index++;
		}		
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

	Column[] columns = null;

	public void setAliasSchema(List<SelectItem> items, Column[] schema_local) {
		/************ Code for alias **************/

		List<Column> list_column = new ArrayList<>();

		for (SelectItem item : items) {
			if(item instanceof AllColumns) {
				list_column.addAll(Arrays.asList(schema_local));
			}
			else if(item instanceof AllTableColumns) {
				AllTableColumns allTableColumns = (AllTableColumns)item;
				list_column.addAll(Arrays.asList(QuerySolver.schemaCollection.get(allTableColumns.getTable().getName())));
			}
			else if(item instanceof SelectExpressionItem) {
				SelectExpressionItem expressionItem = (SelectExpressionItem)item;
				Expression expression = expressionItem.getExpression();

				String alias = ((SelectExpressionItem) item).getAlias();

				if(expression instanceof Function) {
					Column column_alias = new Column(null, alias);
					list_column.add(column_alias);
				}
				else if(expression instanceof Column) {
					Column current_column = ((Column)expression);
					for(Column column : schema_local) {

						if( current_column.equals(column) ) {
							if( alias != null ) {
								Column column_alias = new Column(column.getTable(), alias);
								column = column_alias;
							}
							list_column.add(column);
							break;
						}
					}
				}
			}
		}

		if(list_column.isEmpty()) {
			columns = schema;
		}
		else {
			columns = new Column[list_column.size()];
			columns = list_column.toArray(columns);
		}

		/************ Code for alias **************/
	}

	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		setAliasSchema(items, schema);
		return columns;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return child.hasNext();
	}

	@Override
	public void restart() {
		child.restart();
	}

	@Override
	public List<Column> getColumn() {
		// TODO Auto-generated method stub
		List<Column> columns = new ArrayList<>();
		for (SelectItem item : this.items) {
			if(item instanceof AllColumns) {
				columns.addAll(Arrays.asList(schema));
			}
			else if(item instanceof AllTableColumns) {
				AllTableColumns allTableColumns = (AllTableColumns)item;
				columns.addAll(Arrays.asList(QuerySolver.schemaCollection.get(allTableColumns.getTable().getName())));
			}
			else if(item instanceof SelectExpressionItem) {
				SelectExpressionItem expressionItem = (SelectExpressionItem)item;
				Expression expression = expressionItem.getExpression();

				if(expression instanceof Function) {
					Function function = (Function)expression;

					if(function.isAllColumns()) {
						continue;
					}
					else {
						for (Expression aggregate : function.getParameters().getExpressions()) {

							Eval eval = new Eval() {
								@Override
								public PrimitiveValue eval(Column arg0) throws SQLException {
									// TODO Auto-generated method stub
									columns.add(arg0);
									return null;
								}
							};

							try{
								eval.eval(aggregate);
							}
							catch (Exception e) {
								// TODO: handle exception
							}
						}
					}

				}
				else if(expression instanceof Column) {
					Column current_column = ((Column)expression);
					columns.add(current_column);
				}
			}
		}


		return columns;
	}
	
	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		return child.getEstimate();
	}
}
