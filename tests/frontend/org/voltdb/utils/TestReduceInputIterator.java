package org.voltdb.utils;

import java.util.Iterator;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import edu.brown.statistics.Histogram;
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
    private Histogram<String> keyHistogram = new Histogram<String>(); 
    
    @Override
    protected void setUp() throws Exception {
        for (int i = 0; i < NUM_ROWS; i++) {
            String name="Jason";
            if(i <3) name="Jason00";
            else if(i <5) name="David01";
            else name = "Tomas77";
            keyHistogram.put(name);
            
            long ct = 99;
            Object row[] = {name,ct};
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
        
        Histogram<String> actual = new Histogram<String>();
        while (rows.hasNext()) {
            String key = rows.getKey();
            this.reduce(key, rows);
            actual.put(key);
        }
        
        for (String key : keyHistogram.values()) {
            assertEquals(keyHistogram.get(key), actual.get(key));
        }
    }
    
    public void reduce(String key, Iterator<VoltTableRow> rows) {
        long count = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            count++;
        } // FOR
        
//        while(rows.hasNext()) {
//            count++;
//        }
        System.out.println("key is: " + key);
        Object new_row[] = {
            key,
            count
        };
        
        //System.out.println("key is: " + key);
        this.reduceOutput.addRow(new_row);
    }
   
    
   

}
