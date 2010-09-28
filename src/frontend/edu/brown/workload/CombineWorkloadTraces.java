package edu.brown.workload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

/**
 * @author pavlo
 */
public class CombineWorkloadTraces {
    private static final Logger LOG = Logger.getLogger(CombineWorkloadTraces.class);
    
    /**
     * Combine a bunch of workloads into a single output stream (sorted by txn timestamps)
     * @param output
     * @param catalog_db
     * @param workloads
     */
    @SuppressWarnings("unchecked")
    public static void combineWorkloads(OutputStream output, Database catalog_db, AbstractWorkload workloads[]) {
        Integer next_idxs[] = new Integer[workloads.length];
        Integer max_idxs[] = new Integer[workloads.length];
        List<TransactionTrace> txns[] = new List[workloads.length];
        long relative_starts[] = new long[workloads.length];
        for (int i = 0; i < workloads.length; i++) {
            txns[i] = workloads[i].getTransactions().asList();
            next_idxs[i] = 0;
            max_idxs[i] = txns[i].size();
            relative_starts[i] = txns[i].get(0).getStartTimestamp();
            LOG.info(String.format("Workload #%02d: %d txns", i, txns[i].size()));
        }
        Histogram proc_histogram = new Histogram(); 
        
        // This is basically a crappy merge sort...
        long ctr = 0;
        long trace_id = 1;
        while (true) {
            long min_timestamp = Long.MAX_VALUE;
            Integer min_idx = null;
            for (int i = 0; i < workloads.length; i++) {
                if (next_idxs[i] == null) continue;
                TransactionTrace xact = txns[i].get(next_idxs[i]); 
                // System.err.println("[" + i + "] " + xact + " - " + xact.getStartTimestamp());
                long start = xact.getStartTimestamp() - relative_starts[i]; 
                if (start < min_timestamp) {
                    min_timestamp = start; 
                    min_idx = i;
                }
            } // FOR
            if (min_idx == null) break;
            
            // Insert the txn into the output
            int current_offset = next_idxs[min_idx];
            TransactionTrace xact = txns[min_idx].get(current_offset);
            
            // Update trace ids so that we don't get duplicates
            // Fix the timestamps so that they are all the same
            xact.id = trace_id++;
            xact.start_timestamp -= relative_starts[min_idx];
            xact.stop_timestamp -= relative_starts[min_idx];
//            if (next_idxs[min_idx] == 0) System.err.println(xact.debug(catalog_db));
            for (QueryTrace query_trace : xact.getQueries()) {
                query_trace.id = trace_id++;
                query_trace.start_timestamp -= relative_starts[min_idx];
                query_trace.stop_timestamp -= relative_starts[min_idx];
            } // FOR
            WorkloadTraceFileOutput.writeTransactionToStream(catalog_db, xact, output);
            proc_histogram.put(xact.getCatalogItemName());
            
            // And increment the counter for the next txn we could use from this workload
            // If we are out of txns, set next_txns to null
            if (++next_idxs[min_idx] >= max_idxs[min_idx]) {
                next_idxs[min_idx] = null;
                LOG.info(String.format("Successfully merged all txns for Workload #%02d", min_idx));
            }
            ctr++;
        } // WHILE
        LOG.info("Successfull merged all " + ctr + " txns");
        LOG.info("Procedures Histogram:\n" + proc_histogram);
        return;
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD_OUTPUT);
        
        String output_path = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT);
        
        List<File> workload_files = new ArrayList<File>();
        for (int i = 0, cnt = args.getOptParamCount(); i < cnt; i++) {
            File base_workload_path = new File(args.getOptParam(i));
            File base_directory = base_workload_path.getParentFile();
            String base_workload_name = base_workload_path.getName();
            
            workload_files.addAll(FileUtil.getFilesInDirectory(base_directory, base_workload_name));
            if (workload_files.isEmpty()) {
                LOG.fatal("No workload files starting with '" + base_workload_name + "' were found in '" + base_directory + "'");
                System.exit(1);
            }
        }
        Collections.sort(workload_files);
        
        int num_workloads = workload_files.size();
        WorkloadTraceFileOutput workloads[] = new WorkloadTraceFileOutput[num_workloads];
        LOG.info("Combining " + num_workloads + " workloads into '" + output_path + "'");
        for (int i = 0; i < num_workloads; i++) {
            File input_path = workload_files.get(i);
            LOG.info("Loading workload '" + input_path + "'");
            workloads[i] = new WorkloadTraceFileOutput(args.catalog);
            workloads[i].load(input_path.getAbsolutePath(), args.catalog_db);
        } // FOR

        FileOutputStream output = new FileOutputStream(output_path);
        combineWorkloads(output, args.catalog_db, workloads);
        output.close();
    }

}