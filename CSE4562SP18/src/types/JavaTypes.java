package types;


public class JavaTypes {
	
	public static final int StringValue = 0; 
	public static final int LongValue = 1;
	public static final int DoubleValue = 2;
	public static final int DateValue = 3;
	
	public static int getJavaType(String toBeConverted) {

		//System.out.println(colDataType);

		switch (toBeConverted.toLowerCase()) {
		case "string":
			return StringValue;

		case "varchar":
			return StringValue;

		case "char":
			return StringValue;

		case "int":
			return LongValue;

		case "integer":
			return LongValue;

		case "decimal":
			return DoubleValue;

		case "double":
			return DoubleValue;

		case "date":
			return DateValue;

		default:
			break;
		}
		return StringValue;
	}
}
