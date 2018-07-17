package optimizers;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import projectionsClasses.Projection;
import runnableClasses.JoinProduct;
import runnableClasses.Selection;
import runnableClasses.TableAttributes;
import runnableClasses.TreeNode;

public class OptimizeProjection {
	
	public static boolean optimize(TreeNode root) {

		if(root == null)
			return false;

		TreeNode current = root;
		List<Column> columns = getColList(current);
		//System.err.println("All the cols are " + columns);
		
		current = root;
	
		OptimizeProjection.setSelection(current, columns, null, true);
		
		return true;
	}
	
	static List<Column> getColList(TreeNode current){
		
		List<Column> columns = new ArrayList<>(), temp = null;

		while(current != null) {
			
			if(current instanceof JoinProduct) {
				JoinProduct joinProduct = (JoinProduct)current;
				columns.addAll(OptimizeProjection.getColList(joinProduct.getRight()));
			}

			temp = current.getColumn();
			if(temp != null)
				columns.addAll(temp);
			
			current = current.getChild();
		}
		return columns;
	}
	
	
	static boolean setSelection(TreeNode current, List<Column> columns, TreeNode parent, boolean side) {
		TreeNode grandParent = null;
		
		while(current != null) {

			if(current instanceof JoinProduct) {
				JoinProduct joinProduct = (JoinProduct)current;
				OptimizeProjection.setSelection(joinProduct.getRight(), columns, current, false);
			}
			else if( current instanceof TableAttributes ){
				TableAttributes attributes = (TableAttributes)current;
				
				if( attributes.isOptimized() )
					return false;
				
				Column[] tableColumns = current.getNodeSchema();
				List<Column> matchingColumns = new ArrayList<>();
				for(Column tableCol : tableColumns) {
					if(columns.contains(tableCol))
						matchingColumns.add(tableCol);
				}
				
				if( !matchingColumns.isEmpty() ) {
					List<SelectItem> items = new ArrayList<>();
					for(Column col : matchingColumns) {
						SelectExpressionItem item = new SelectExpressionItem();
						item.setExpression(col);
						items.add(item);
					}
					
					//System.err.println("Setting this ---- " + items);
					TreeNode node = new Projection(current, items);
					attributes.setOptimized(true);
					
					if(side) {
						parent.setChild(node);
					}
					else {
						JoinProduct join = (JoinProduct) parent;
						join.setRight(node);
					}
				}
			}
			
			
			if(parent != null) {
				grandParent = parent;
			}

			parent = current;
			current = current.getChild();
			side = true;
		}
		
		return true;
	}
}