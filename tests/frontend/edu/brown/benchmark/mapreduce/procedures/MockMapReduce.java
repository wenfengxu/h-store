package edu.brown.benchmark.mapreduce.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
	mapInputQuery = "mapInputQuery"
)
public class MockMapReduce extends VoltMapReduceProcedure {

    public SQLStmt mapInputQuery = new SQLStmt(
        "SELECT A_NAME FROM TABLEA WHERE A_AGE >= ?"
//		"SELECT A_NAME, COUNT(*) FROM TABLEA WHERE A_AGE >= ? GROUP BY A_NAME"
	);

    public SQLStmt reduceInputQuery = new SQLStmt(
		"SELECT * FROM TABLEA"
	);

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
            new VoltTable.ColumnInfo("NAME", VoltType.STRING),
            new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        };
    }
    
    @Override
    public void map(VoltTableRow row) {
        Object new_row[] = {
            row.getString(0),   // A_NAME
            1, // FIXME row.getLong(1)      // COUNT(*)
        };
        this.mapEmit(new_row);
    }
    
    @Override
    public void reduce(VoltTable[] reduceInputTable) {
        String newName = "";
        String oldName = "";
        long value = 0;
        while (reduceInputTable[0].advanceRow()) {
            newName = reduceInputTable[0].getString(0);
            if (oldName.equals(oldName)) {
                value += (long) reduceInputTable[0].getLong(1);
                oldName = newName;
            } else {
                Object new_row[] = { reduceInputTable[0].getString(0), value };
                this.reduceEmit(new_row);
                value = 0;
            }
        } // WHILE
    }

}
