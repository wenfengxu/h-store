package edu.brown.benchmark.locality.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

@ProcInfo(
    mapReduce = true,
		
    mapInputQuery = "mapInputQuery",
    mapEmitTable = "MR_MOCK_MAP",
    reduceInputQuery = "reduceInputQuery",
	reduceEmitTable = "MR_MOCK_REDUCE"
)
public class MockMapReduce extends VoltMapReduceProcedure {

    public SQLStmt mapInputQuery = new SQLStmt("SELECT * FROM TABLEA");
    
    public SQLStmt reduceInputQuery = new SQLStmt("SELECT * FROM MR_MOCK_MAP");
    
    
   @Override
    public void map(VoltTableRow row) {
	   Object new_row[] = { row.getString(1), 1 };
	   this.mapEmit(new_row);
   }
    
    @Override
    public void reduce(VoltTable[] reduceInputTable) {
    	String newName = "";
    	String oldName = "";
    	long value = 0;
    	while (reduceInputTable[0].advanceRow()) {
    		newName = reduceInputTable[0].getString(0);
    		if(oldName.equals(oldName)){
    			value += (long) reduceInputTable[0].getLong(1);
    			oldName = newName;
    		}
    		else{
    			Object new_row[] = { reduceInputTable[0].getString(0), value };
 		   		this.reduceEmit(new_row);
 		   		value = 0;
    		}
 	   } // WHILE
    }
    
}
