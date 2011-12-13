package org.voltdb.utils;

import java.util.Iterator;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import edu.brown.utils.CollectionUtil;

import junit.framework.TestCase;

public class TestReduceInputIterator extends TestCase {
    
    static final VoltTable.ColumnInfo[] SCHEMA = new VoltTable.ColumnInfo[] {
        //new VoltTable.ColumnInfo("ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        //new VoltTable.ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    static final int NUM_ROWS = 10;
    static final Random rand = new Random();

    private VoltTable table = new VoltTable(SCHEMA);
    private VoltTable reduceOutput = new VoltTable(SCHEMA);
    @Override
    protected void setUp() throws Exception {
        for (int i = 0; i < NUM_ROWS; i++) {
            Object row[] = new Object[SCHEMA.length];
            for (int j = 0; j < row.length; j++) {
                row[j] = VoltTypeUtil.getRandomValue(SCHEMA[j].getType());
            } // FOR
            this.table.addRow(row);
        } // FOR
        assertEquals(NUM_ROWS, this.table.getRowCount());
    }
    
    /**
     * testHasKey
     */
    public void testHasKey() throws Exception {
        ReduceInputIterator<Long> iterator = new ReduceInputIterator<Long>(this.table);
        assertNotNull(iterator);
        assertTrue(iterator.hasKey());
        
        //System.err.println(this.table);
    }
    
    /*
     * test hasNext
     */
    public void testHasNext() throws Exception {
        ReduceInputIterator<String> rows = new ReduceInputIterator<String>(this.table);
        assertNotNull(rows);
        
        while (rows.hasKey()) {
            String key = rows.getKey();
            this.reduce5(key, rows); 
        }
        
    }
    
    public void reduce5(String key, Iterator<VoltTableRow> rows) {
        long count = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            
            count++;
           
        } // FOR
        
        Object new_row[] = {
            key,
            count
        };
        
        System.out.println("key is: " + key);
        this.reduceOutput.addRow(new_row);
    }
    
   

}
