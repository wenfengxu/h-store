package edu.brown.benchmark.locality.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
    mapReduce = true,
		
    mapInputQuery = "MAP_selectAll_input",
    mapEmitTable = "MR_MOCK_MAP",
    
    reduceInputQuery = "REDUCE_query",
	reduceEmitTable = "MR_MOCK_REDUCE"
)
public class MockMapReduce extends VoltMapReduceProcedure {

    public SQLStmt MAP_selectAll_input = new SQLStmt("SELECT * FROM TABLEA WHERE value > ? AND blah = ? AND xin = ?");
    
    @Override
    public void map(VoltTableRow row) {
    	voltQueueSQL(MAP_selectAll_input);
    	VoltTable results[] = voltExecuteSQL();
    	while (results[0].advanceRow()) {
    		Object new_row[] = { results[0].getString(1), 1 };
    		this.mapEmit(new_row);
    	} // WHILE
    	
//        return (output);
    }
    
    //SQLStmt REDUCE_query = new SQLStmt("SELECT * FROM MR_MOCK");
//    public SQLStmt REDUCE_query = new SQLStmt("SELECT * FROM TABLEB");
    @Override
    public void reduce(VoltTableRow row) {
    	    	
        
//        return (new VoltTable[0]);//why??? new VoltTable[0]
    }	
    
}
