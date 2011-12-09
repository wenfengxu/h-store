package org.voltdb.utils;

import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import junit.framework.TestCase;

public class TestReduceInputIterator extends TestCase {
    
    static final VoltTable.ColumnInfo[] SCHEMA = new VoltTable.ColumnInfo[] {
        new VoltTable.ColumnInfo("ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        new VoltTable.ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    static final int NUM_ROWS = 10;
    static final Random rand = new Random();

    private VoltTable table = new VoltTable(SCHEMA);
    
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
        
        System.err.println(this.table);
    }

}
