package edu.buffalo.www.cse4562;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import runnableClasses.TreeNode;
import types.TupleRecord;

public class Main {

	static String prompt = "$> "; // expected prompt

	public static void main(String[] args) throws ParseException, SQLException {

		try {

			System.out.println(prompt);
			System.out.flush();


			Reader in = new InputStreamReader(System.in);
			CCJSqlParser parser = new CCJSqlParser(in);
			Statement statement;

			int i = 0;

			while((statement = parser.Statement()) != null){

				long startTime = System.currentTimeMillis();

				QuerySolver q = new QuerySolver(statement);

				TreeNode root = q.buildPlan();


				if(q.getQueryType() == QuerySolver.SELECT) {

					TupleRecord tuple = null;
					while( ( tuple = root.nextTuple(null, null, false) ) != null) {

						if( tuple != null) {
							List<PrimitiveValue> primitiveValues = tuple.getAll();
							String[] stringToBePrinted = new String[primitiveValues.size()];
							i = 0;
							for(PrimitiveValue value : primitiveValues)
								stringToBePrinted[i++] = value.toString();
							System.out.println(String.join("|", stringToBePrinted));
//							System.err.println(String.join("|", stringToBePrinted));
						}
					}
				}
				long endTime = System.currentTimeMillis();
				System.err.println(endTime - startTime);
				System.out.println(prompt);
				System.out.flush();
				System.gc();
			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Parser exception!");
			e.printStackTrace();
		}
	}
	//		(new Output()).printOutput();
	//		


	//for(String file : args){
	//Reader input;
	//if(file.equals("-")){
	// input = new InputStreamReader(System.in);
	//} else {
	// input = new FileReader(file);
	//}
	//		System.out.println(prompt);
	//        System.out.flush();
	//        Reader in = new InputStreamReader(System.in);
	//		      CCJSqlParser parser = new CCJSqlParser(in);
	//		      Eval eval = new Eval() {
	//				
	//				@Override
	//				public PrimitiveValue eval(Column arg0) throws SQLException {
	//					// TODO Auto-generated method stub
	//					//return null;
	//					return net.sf.jsqlparser.expression.BooleanValue.FALSE;
	//				}
	//			};
	//		  //      new Eval() {
	//	          //public PrimitiveValue eval(Column c){ return new LongValue(0); }
	//		   //     };
	//		      Expression e = parser.SimpleExpression();
	//		      PrimitiveValue ret = eval.eval(e);
	//		      System.out.println(ret.toString());
	//		      
	//		      //}
	//		    return;



	//System.out.println("Program end!");

}
