package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import runnableClasses.SubQuery;
import runnableClasses.TableAttributes;
import runnableClasses.TreeNode;

public class ExecuteFromItem implements FromItemVisitor{

	TreeNode node = null;
	
	@Override
	public void visit(Table tables) {
		// TODO Auto-generated method stub
		//System.out.println("Tables has been called ... ");
		//String tableName = tables.getName();
		//System.out.println("alias " + tables.getAlias());
		this.node = new TableAttributes(tables.getName(), tables.getAlias());
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		//arg0.accept(this);
		//System.out.println("SubSelect has been called ... ");
		try {
			this.node = new SubQuery( arg0);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub	
		System.out.println("Something went wrong....");
		
	}

	public TreeNode getNode() {
		return node;
	}
}
