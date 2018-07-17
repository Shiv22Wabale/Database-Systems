package types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;

public class TupleRecord implements Serializable {
	
	static final long serialVersionUID = 42L;
	
	CSVRecord csvRecord;
	int[] type;
	List<PrimitiveValue> primitiveValues;

	public TupleRecord(int size) {
		primitiveValues = new ArrayList<PrimitiveValue>();
	}

	public TupleRecord(List<PrimitiveValue> primitiveValues) {
		this.primitiveValues = primitiveValues;
	}

	public TupleRecord(CSVRecord record,int[] type) {
		this.csvRecord = record;
		this.type = type;
		primitiveValues = new ArrayList<PrimitiveValue>();
		for(int i = 0; i < record.size(); i++){
			primitiveValues.add(null);
		}
	}

	public void addAll(List<PrimitiveValue> primitiveValues) {
		this.primitiveValues.addAll( primitiveValues );
	}

	public List<PrimitiveValue> getAll() {
		for(int i=0;i < primitiveValues.size(); i++) {
			if(primitiveValues.get(i) == null) {
				primitiveValues.set(i, getPrimitive(csvRecord.get(i), type[i]));
			}
		}
		return primitiveValues;
	}

	public PrimitiveValue getRecord(int i) {
		if(primitiveValues.get(i) == null) {
			try {
			primitiveValues.set(i, getPrimitive(csvRecord.get(i), type[i]));
			}
			catch (NumberFormatException e) {
				// TODO: handle exception
				e.printStackTrace();
				List<String> stringList = new ArrayList<>();
				
				for(int j = 0; j < csvRecord.size(); j++){
					stringList.add(csvRecord.get(j));
				}
				
				String csvString = String.join("|", stringList);
				System.err.println(" the error occurred " + csvString + " for " + csvRecord);
			}
		}
		return primitiveValues.get(i);
	}

	public int size() {
		return primitiveValues.size();
	}

	public int[] getType() {
		return type;
	}

	private PrimitiveValue getPrimitive(String toBeConverted, int colDataType) {

		switch (colDataType) {

		case JavaTypes.StringValue:
			return new StringValue(toBeConverted);

		case JavaTypes.LongValue:
			return new LongValue(toBeConverted);

		case JavaTypes.DoubleValue:
			return new DoubleValue(toBeConverted);

		case JavaTypes.DateValue:
			return new DateValue(toBeConverted);

		default:
			break;
		}

		return new LongValue(55555555);
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.getAll().hashCode();
	}
	
	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		return this.getAll().equals( ( (TupleRecord)arg0 ).getAll() );
	}

}
