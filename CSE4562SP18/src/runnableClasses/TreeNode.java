package runnableClasses;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import types.TupleRecord;

public interface TreeNode {
	
	public void open(Column primary);
	
	public boolean hasNext();
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once);
	
	public Column[] getNodeSchema();
	
	public void restart();
	
	public TreeNode getChild();
	
	public void setChild(TreeNode node);
	
	public List<Column> getColumn();
	
	public int getEstimate();
	//public ColDataType[] getNodeSchemaType(); 
}