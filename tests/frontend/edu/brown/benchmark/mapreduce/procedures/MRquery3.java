package edu.brown.benchmark.mapreduce.procedures;

import java.util.Iterator;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.utils.CollectionUtil;

@ProcInfo(
        mapInputQuery = "mapInputQuery"
)
public class MRquery3 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
            //        "SELECT A_NAME FROM TABLEA WHERE A_AGE >= ?"
            "SELECT ol_o_id, ol_w_id, ol_d_id, ol_amount, o_entry_d, " +
            "FROM orderline, neworder, orders, orderline WHERE c_state like ? " +
            "and c_id = o_c_id " +
            "and c_w_id = o_w_id " +
            "and c_d_id = o_d_id " +
            "and no_w_id = o_w_id " +
            "and no_d_id = o_d_id " +
            "and no_o_id = o_id " +
            "and ol_w_id = o_w_id " +
            "and ol_d_id = o_d_id " +
            "and ol_o_id = o_id " 
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("o_entry_d", VoltType.BIGINT),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("revenue", VoltType.BIGINT),
                new VoltTable.ColumnInfo("o_entry_d", VoltType.TIMESTAMP),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0); // A_NAME
        long ol_w_id = row.getLong(1);
        long ol_d_id = row.getLong(2);
        double ol_amount = row.getDouble(3);
        TimestampType ol_entry_d = row.getTimestampAsTimestamp(4);
        
        Object new_row[] = {
                key,
                ol_w_id, // FIXME row.getLong(1)
                ol_d_id,
                ol_amount,
                ol_entry_d
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        double sum_ol_amount = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_ol_amount += rows.next().getDouble(3);
        } // FOR

        Object new_row[] = {
                key,
                rows.next().getLong(1),
                rows.next().getLong(2),
                sum_ol_amount,
                rows.next().getTimestampAsTimestamp(4)
        };
        this.reduceEmit(new_row);
    }

}


