package types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class TableStats {
	
	static public List<String> primaryKeys = new ArrayList<>();

	public int totalCount;
	public Column primary = null;
	public List<String> primaryComposite = new ArrayList<String>();
	public List<Column> columnRef = new ArrayList<>();
	public HashMap<PrimitiveValue, Long> primaryIndex = new HashMap<>();
	public HashSet<Long> primarySet = new HashSet<>();

	public TableStats() {
		totalCount = 0;
	}	

}