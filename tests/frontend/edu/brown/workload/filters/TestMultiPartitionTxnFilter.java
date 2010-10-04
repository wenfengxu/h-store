package edu.brown.workload.filters;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TestMultiPartitionTxnFilter extends AbstractTestFilter {
    
    /**
     * testSinglePartition
     */
    @Test
    public void testSinglePartition() throws Exception {
        MultiPartitionTxnFilter filter = new MultiPartitionTxnFilter(p_estimator, true);
        
        Iterator<AbstractTraceElement<? extends CatalogType>> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that this txn's base partition is what we expect it to be
                TransactionTrace txn = (TransactionTrace)element;
                Set<Integer> partitions = p_estimator.getAllPartitions(txn);
                assertNotNull(partitions);
                assertEquals(1, partitions.size());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
    
    /**
     * testMultiPartition
     */
    @Test
    public void testMultiPartition() throws Exception {
        MultiPartitionTxnFilter filter = new MultiPartitionTxnFilter(p_estimator, false);
        
        Iterator<AbstractTraceElement<? extends CatalogType>> it = workload.iterator(filter);
        assertNotNull(it);

        int count = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Make sure that this txn's base partition is what we expect it to be
                TransactionTrace txn = (TransactionTrace)element;
                Set<Integer> partitions = p_estimator.getAllPartitions(txn);
                assertNotNull(partitions);
                assertNotSame(1, partitions.size());
                count++;
            }
        } // WHILE
        assert(count > 0);
    }
}