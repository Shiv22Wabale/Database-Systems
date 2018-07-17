package runnableClasses;

import java.io.StringReader;
import java.util.List;

import edu.buffalo.www.cse4562.QuerySolver;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SubSelect;
import types.TupleRecord;

public class SubQuery implements TreeNode {

	TreeNode node;
	String tableAlias = null;
	
	public SubQuery(SubSelect subQuery) throws ParseException {
		
		CCJSqlParser parser = new CCJSqlParser( new StringReader( subQuery.getSelectBody().toString() ) );
		Statement statement = parser.Statement();
				
		QuerySolver query = new QuerySolver(statement) ;
		node = query.buildPlan();
		
		tableAlias = subQuery.getAlias();
		
	}
	
	@Override
	public void open(Column primary) {
		node.open(primary);
	}
	
	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {
		// TODO Auto-generated method stub
		return node.nextTuple(primary, condition, once);
	}

	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		Column[] columns = node.getNodeSchema();
		
		if(tableAlias != null) {
			Column[] list_column = new Column[columns.length];
			int index = 0;
			for(Column column: columns) {
				list_column[index++] = new Column(new Table(tableAlias), column.getColumnName());
				
			}
			return list_column;
		}
		return columns;
	}

	@Override
	public void restart() {
		// TODO Auto-generated method stub
		node.restart();
	}
	
	@Override
	public TreeNode getChild() {
		// TODO Auto-generated method stub
		return node.getChild();
	}

	@Override
	public void setChild(TreeNode node) {
		// TODO Auto-generated method stub
		node.setChild(node);
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return node.hasNext();
	}

	@Override
	public List<Column> getColumn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		return node.getEstimate();
	}
}