package runnableClasses;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import net.sf.jsqlparser.statement.select.OrderByElement;
import optimizers.Optimize;
import types.TupleRecord;

public class OrderBy implements TreeNode{
	TreeNode child;
	Column[] schema;

	List<OrderByElement>  orderByElementsList=null;
	//List<PrimitiveValue[]> sortElements;

	//static boolean sorted = false;
	boolean first = true;
	int index = 0;

	//int i=0;

	private List<TupleRecord> listA;

	public OrderBy(TreeNode child, List<OrderByElement> orderByElementsList) {
		this.child = child;
		this.orderByElementsList = orderByElementsList;
		//		sort();

		//System.err.println(orderByElementsList);
		
		//Optimize.optimize(child);
		
		index = 0;
		first = true;
		
		//System.out.println(listA);
//		System.err.println("---------Testing-----------");
//		System.err.println("Before Sort");
//		for(PrimitiveValue[] primitiveValues : listA) {
//			for(PrimitiveValue primitiveValue : primitiveValues)
//				System.err.print(primitiveValue + " | ");
//			System.err.println();
//		}
		
		//Collections.sort(listA, new PrimitiveValueComparator(orderByElementsList, schema));

//		System.err.println("After Sort");
//
//		for(PrimitiveValue[] primitiveValues : listA) {
//			for(PrimitiveValue primitiveValue : primitiveValues)
//				System.err.print(primitiveValue + " | ");
//			System.err.println();
//		}
//		System.err.println("---------Testing-----------");
	}


	@Override
	public void open(Column primary) {
		child.open(primary);
		this.schema = child.getNodeSchema();
	}

	@Override
	public TupleRecord nextTuple(Column primary, PrimitiveValue condition, boolean once) {
		//	List<PrimitiveValue> output = new ArrayList<PrimitiveValue>();
		//tuple = child.nextTuple();

		if(first) {
			listA = new ArrayList<>();
			TupleRecord tuple = null;
			while( ( tuple = child.nextTuple(primary, condition, once) ) != null) {
				
				if( tuple != null) {
					listA.add(tuple);
				}
			}
			
			Collections.sort(listA, new PrimitiveValueComparator(orderByElementsList, schema));
			
			first = false;
		}
		
		if(listA.size() <= index)
			return null;
		return listA.get(index++);

		/*		if(tuple == null )
			return null;
		while (child.hasNext()){

			output.addAll( Arrays.asList(tuple) );
			System.out.println("In order by "+tuple);
			sortElements.add(tuple);
			System.out.println(sortElements);
			tuple = child.nextTuple();

		//	System.out.println(i);

		}
		System.out.println(output);	

		PrimitiveValue[] primitiveOutput = new PrimitiveValue[output.size()];
		primitiveOutput = output.toArray(primitiveOutput);
		return primitiveOutput;	*/

	}



	@Override
	public Column[] getNodeSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

	@Override
	public void restart() {
		// TODO Auto-generated method stub
		index = 0;
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