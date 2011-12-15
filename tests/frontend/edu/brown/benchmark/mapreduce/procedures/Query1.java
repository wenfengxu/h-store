package edu.brown.benchmark.mapreduce.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * 
 * @author xin
 */
@ProcInfo(
    partitionInfo = "TABLEA.A_ID: 0",
    singlePartition = true
)
public class Query1 extends VoltProcedure {

    public final SQLStmt Query1 = new SQLStmt(
            "select   ol_number, " + 
            "sum(ol_quantity) as sum_qty, " + 
            "sum(ol_amount) as sum_amount, " + 
            "avg(ol_quantity) as avg_qty, " + 
            "avg(ol_amount) as avg_amount, " + 
            "count(*) as count_order " + 
            "from     order_line " + 
            "where    ol_delivery_d > ? " + 
            "group by ol_number order by ol_number ");
   
    /**
     * @param a_id
     * @return
     */
    public VoltTable[] run(long a_id) {
        voltQueueSQL(Query1, a_id);
        
        return (voltExecuteSQL());
    }
    
}
