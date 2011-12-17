package org.voltdb.benchmark.tpcc.procedures;

import java.util.Iterator;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import edu.brown.utils.CollectionUtil;

@ProcInfo(
        mapInputQuery = "mapInputQuery"
)
public class MRquery19 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
              //   "SELECT A_NAME FROM TABLEA WHERE A_AGE >= ?"
            "select ol_w_id, ol_amount as revenue " +
            "from   order_line, item " +
            "where ol_quantity >=  ? " +
            "group by ol_w_id"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("revenue", VoltType.BIGINT),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0); 
        Object new_row[] = {
                key,
                row.getDouble(1)
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        long sum_ol_amount = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_ol_amount += rows.next().getDouble(1);
        } // FOR

        Object new_row[] = {
                key,
                sum_ol_amount
        };
        this.reduceEmit(new_row);
    }

}



