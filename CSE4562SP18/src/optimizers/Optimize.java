package optimizers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import runnableClasses.JoinProduct;
import runnableClasses.Selection;
import runnableClasses.TreeNode;

public class Optimize {

	public static boolean optimize(TreeNode root, TreeNode grandParent, boolean side) {

		if(root == null)
			return false;

		boolean optimized = true;


		while( optimized ) {

			System.err.println("Optimizing.....");

			optimized = false;

			TreeNode current = root, parent = null;

			while(current != null) {

				//System.err.println("Optimizing..... " + current.getClass().getName() + " it's child is " + current.getChild());
				
				if(current instanceof JoinProduct) {

					JoinProduct joinProduct = (JoinProduct)current;

					if(parent instanceof Selection) {

						
						Selection selection = (Selection)parent;
						Expression selectionExpression = selection.getExpression();
						
						//System.err.println("Selection expression is " + selectionExpression);
						
						if(selectionExpression != null) {

							List<Expression> expressions = getExpressionList(selectionExpression);
							
//							if(expressions.size() == 1) {
//								
////								if(side)
////									grandParent.setChild(joinProduct);
////								else {
////									JoinProduct join = (JoinProduct) grandParent;
////									join.setRight(joinProduct);
////								}
////								
////								joinProduct.setJoinExpression( expressions.get(0) );
////								selection.setExpression(null);
//							}
							//else if(selectionExpression instanceof AndExpression) {
							if( expressions.size() > 0 ) {
								List<Expression> expressionsRemaning = new ArrayList<>();

								List<Expression> left = new ArrayList<>();
								List<Expression> right = new ArrayList<>();
								List<Expression> joinExpressions = new ArrayList<>();


								expressionsRemaning.addAll(expressions);

//								System.err.println(" Checking expressions --- " + expressions );
								
								for(Expression expression : expressions ) {
									
									if(expressionMatches(expression, joinProduct.getChild().getNodeSchema())) {
										left.add(expression);
										expressionsRemaning.remove(expression);
									}
									else if(expressionMatches(expression, joinProduct.getRight().getNodeSchema())) {
										right.add(expression);
										expressionsRemaning.remove(expression);
									}
									if(expressionMatches(expression, joinProduct.getChild().getNodeSchema())) {
										left.add(expression);
										expressionsRemaning.remove(expression);
									}
									else if(expression instanceof EqualsTo) {
										EqualsTo equalsTo = (EqualsTo)expression;
										Expression leftExpression = equalsTo.getLeftExpression();
										Expression rightExpression = equalsTo.getRightExpression();
										
										if( ( expressionMatches(leftExpression, joinProduct.getChild().getNodeSchema()) 
												&& expressionMatches(rightExpression, joinProduct.getRight().getNodeSchema()) ) 
												|| ( expressionMatches(rightExpression, joinProduct.getChild().getNodeSchema()) 
														&& expressionMatches(leftExpression, joinProduct.getRight().getNodeSchema()) ) ) {
											if( joinExpressions.isEmpty() ) {
												joinExpressions.add(expression);
												expressionsRemaning.remove(expression);
											}
										}
										
//										System.err.println("The equals to check for join " + equalsTo);
//										
//										System.err.println("Checking with left " + Arrays.asList(joinProduct.getChild().getNodeSchema()) );
//										System.err.println("Checking with right " + Arrays.asList(joinProduct.getRight().getNodeSchema()) );
//										
//										System.err.println("The matched expression " + joinExpressions);
//										right.add(expression);
//										expressionsRemaning.remove(expression);
									}
								}

								Expression expressionForSelection = null;
								if( !left.isEmpty() ) {
									
									//optimized = true;
									
									expressionForSelection = left.get(0); 
									for(int i = 1; i < left.size(); i++) {
										AndExpression temp = new AndExpression();
										temp.setLeftExpression(expressionForSelection);
										temp.setRightExpression(left.get(i));

										expressionForSelection = temp;
									}
									
									//System.err.println("Setting left " + expressionForSelection);
									
									Selection intermediate = new Selection(joinProduct.getChild(), expressionForSelection); 
									joinProduct.setChild(intermediate);
									
									joinProduct.open(null);
									optimize(intermediate, joinProduct, true);
								}

								expressionForSelection = null;
								if( !right.isEmpty() ) {
									
									//optimized = true;
									
									expressionForSelection = right.get(0); 
									for(int i = 1; i < right.size(); i++) {
										AndExpression temp = new AndExpression();
										temp.setLeftExpression(expressionForSelection);
										temp.setRightExpression(right.get(i));

										expressionForSelection = temp;
									}
									
									//System.err.println("Setting right " + expressionForSelection);
									
									Selection intermediate = new Selection(joinProduct.getRight(), expressionForSelection);
									joinProduct.setRight(intermediate);
									
									joinProduct.open(null);
									optimize(intermediate, joinProduct, false);
								}

								Expression expressionForJoin = null;
								if( !joinExpressions.isEmpty() ) {
									expressionForJoin = joinExpressions.get(0); 
									for(int i = 1; i < expressionsRemaning.size(); i++) {
										AndExpression temp = new AndExpression();
										temp.setLeftExpression(expressionForJoin);
										temp.setRightExpression(expressionsRemaning.get(i));

										expressionForJoin = temp;
									}
								}
								
								Expression expressionForSelect = null;
								if( !expressionsRemaning.isEmpty() ) {
									
									expressionForSelect = expressionsRemaning.get(0);
									
//									System.err.println(expressionForSelect);
									for(int i = 1; i < expressionsRemaning.size(); i++) {
										AndExpression temp = new AndExpression();
										temp.setLeftExpression(expressionForSelect);
										temp.setRightExpression(expressionsRemaning.get(i));

										expressionForSelect = temp;
									}
									
									selection.setExpression(expressionForSelect);
									
									//System.err.println("Optimizing..... 1   " + current.getClass().getName() + " it's child is " + current.getChild());
									
									if(expressionForJoin != null) {
//										System.err.println("Expression for join " + expressionForJoin);
										joinProduct.setJoinExpression( expressionForJoin );
									}
									
									//System.err.println("Optimizing..... 2  " + current.getClass().getName() + " it's child is " + current.getChild());
								}
								else if(expressionForJoin != null) {
//									System.err.println("Expression for join " + expressionForJoin);
									if(side)
										grandParent.setChild(joinProduct);
									else {
										JoinProduct join = (JoinProduct) grandParent;
										join.setRight(joinProduct);
									}
									
									joinProduct.setJoinExpression( expressionForJoin );
									selection.setExpression(null);
								}
							}

						}

					}

					//Optimize.optimize(joinProduct.getRight());
				}

				if(parent != null) {
					grandParent = parent;
				}

				parent = current;
				current = current.getChild();
				
				
			}
		}

		return true;
	}

	private static List<Expression> getExpressionList(Expression expression){
		List<Expression> exp = new ArrayList<>();
		if(expression == null)
			return null;
		if(expression instanceof AndExpression) {
			Expression leftExpression = ((AndExpression) expression).getLeftExpression();
			exp.addAll( getExpressionList(leftExpression) );
			exp.add(((AndExpression) expression).getRightExpression());
		}
		else {
			exp.add(expression);
		}

		return exp;
	}

	private static boolean expressionMatches(Expression expression, Column[] schema) {

		if(expression == null)
			return false;

		if(expression instanceof BinaryExpression) {

			BinaryExpression binaryExpression = (BinaryExpression)expression;
			Expression left = binaryExpression.getLeftExpression(), right=binaryExpression.getRightExpression();

			boolean lefttype = false, righttype = false;


			for(Column column2: schema) {

				if(!lefttype) {
					if( left instanceof Column ) {

						Column column = (Column)left;

						if( column.equals(column2) )
							lefttype = true;

					}
					else if( left instanceof PrimitiveValue ) {
						lefttype = true;
					}
					else {
						return false;
					}
				}
				if(!righttype) {
					if( right instanceof Column ) {
						Column column = (Column)right;
						if(column.equals(column2))
							righttype = true;
					}
					else if( right instanceof PrimitiveValue ) {
						righttype = true;
					}
					else {
						return false;
					}
				}
				if(lefttype && righttype)
					return true;
			}

			return lefttype && righttype;
		}
		else if(expression instanceof Column) {
			for(Column column2: schema) {
				Column column = (Column)expression;
				if(column.equals(column2))
					return true;
			}
		}

		return false;
	}


}
