package runnableClasses;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;
import types.TupleRecord;

public class LimitQuery implements TreeNode{

	TreeNode child;
	Column[] schema;
	
	long limitCounter = 0;
	long finalLimit = 0;
	Limit limit;
	
	public LimitQuery(TreeNode child, Limit limit) {
		this.child = child;
		
		
		this.limit = limit;
		this.limitCounter = 0;
		this.finalLimit = limit.getRowCount();
	}
	

	@Override
	public void open(Column primary) {
		
		child.open(primary);
		this.schema = child.getNodeSchema();		
	}
	
	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {
		// TODO Auto-generated method stub
		TupleRecord tuple;
		if(limitCounter++<finalLimit)
			if( (tuple = child.nextTuple(primary, condition, once)) != null )
				return tuple;
		return null;
	}

	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

	@Override
	public void restart() {
		// TODO Auto-generated method stub
		child.restart();
		limitCounter = 0;
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
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public int getEstimate() {
		// TODO Auto-generated method stub
		return child.getEstimate();
	}

}
