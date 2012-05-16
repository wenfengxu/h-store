/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.RandomProcParameter;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.ColumnSet;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.Histogram;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * @author pavlo
 */
public class PartitionEstimator {
    private static final Logger LOG = Logger.getLogger(PartitionEstimator.class.getName());
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    private Database catalog_db;
    private final AbstractHasher hasher;
    private final Set<Integer> all_partitions = new HashSet<Integer>();
    private int num_partitions;

    private final HashMap<Procedure, ProcParameter> cache_procPartitionParameters = new HashMap<Procedure, ProcParameter>();
    private final Map<Table, Column> cache_tablePartitionColumns = new HashMap<Table, Column>();
    private final Map<Statement, Collection<Integer>> cache_stmtPartitionParameters = new HashMap<Statement, Collection<Integer>>();

    /**
     * PlanFragment Key -> CacheEntry(Column Key -> StmtParameter Indexes)
     */
    private final Map<String, CacheEntry> cache_fragmentEntries = new HashMap<String, CacheEntry>();

    /**
     * Statement Key -> CacheEntry(Column Key -> StmtParam Indexes)
     */
    private final Map<String, CacheEntry> cache_statementEntries = new HashMap<String, CacheEntry>();

    /**
     * Table Key -> All cache entries for Statements that reference Table
     */
    private final Map<String, Set<CacheEntry>> table_cache_xref = new HashMap<String, Set<CacheEntry>>();

    /**
     * CacheEntry ColumnKey -> Set<StmtParameterIndex>
     */
    private class CacheEntry extends HashMap<Column, int[]> {
        private static final long serialVersionUID = 1L;
        private final QueryType query_type;
        private boolean contains_or = false;
        private final Set<String> table_keys = new HashSet<String>();
        private final Collection<String> broadcast_tables = new HashSet<String>();

        private transient Table tables[];
        /** Whether the table in the tables array is replicated */
        private transient boolean is_replicated[];
        private transient boolean is_array[]; // parameters
        private transient boolean is_valid = true;
        private transient boolean cache_valid = false;

        public CacheEntry(QueryType query_type) {
            this.query_type = query_type;
        }

        /**
         * @param key
         * @param param_idx
         * @param catalog_tbls
         */
        public void put(Column key, int param_idx, Table... catalog_tbls) {
            int params[] = this.get(key);
            boolean dirty = true;
            if (params == null) {
                params = new int[]{ param_idx };
            } else {
                for (int idx : params) {
                    if (idx == param_idx) {
                        dirty = false;
                        break;
                    }
                } // FOR
                
                if (dirty) {
                    int temp[] = new int[params.length + 1];
                    System.arraycopy(params, 0, temp, 0, params.length);
                    temp[temp.length-1] = param_idx;
                    params = temp;
                }
            }
            if (dirty) this.put(key, params);
            for (Table catalog_tbl : catalog_tbls) {
                this.table_keys.add(CatalogKey.createKey(catalog_tbl));
            } // FOR
        }

        public void markContainsOR(boolean flag) {
            this.contains_or = flag;
        }

        public boolean isMarkedContainsOR() {
            return (this.contains_or);
        }

        /**
         * The catalog object for this CacheEntry references a table without any
         * predicates on columns, so we need to mark it as having to always be
         * broadcast (unless it is replicated)
         * @param catalog_tbls
         */
        public void markAsBroadcast(Table... catalog_tbls) {
            for (Table catalog_tbl : catalog_tbls) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                this.table_keys.add(table_key);
                this.broadcast_tables.add(table_key);
            } // FOR
        }

        public boolean hasBroadcast() {
            return (this.broadcast_tables.isEmpty() == false);
        }

        /**
         * Get all of the tables referenced in this CacheEntry
         * @return
         */
        public Table[] getTables() {
            if (this.cache_valid == false) {
                // We have to update the cache set if don't have all of the
                // entries we need or the catalog has changed
                synchronized (this) {
                    if (this.cache_valid == false) {
                        if (trace.get())
                            LOG.trace("Generating list of tables used by cache entry");
                        
                        this.tables = new Table[this.table_keys.size()];
                        this.is_replicated = new boolean[this.tables.length];
                        int i = 0;
                        for (String table_key : this.table_keys) {
                            Table catalog_tbl = CatalogKey.getFromKey(catalog_db, table_key, Table.class);
                            this.tables[i] = catalog_tbl;
                            this.is_replicated[i++] = catalog_tbl.getIsreplicated();
                        } // FOR
                    }
                    this.cache_valid = true;
                } // SYNCH
            }
            return (this.tables);
        }

        public boolean hasTable(Table catalog_tbl) {
            return (this.table_keys.contains(CatalogKey.createKey(catalog_tbl)));
        }
        public void setValid() {
            this.is_valid = true;
        }
        public boolean isValid() {
            return (this.is_valid);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[IsValid=" + this.is_valid + ", ").append("Tables=" + this.table_keys + ", ").append("Broadcast=" + this.broadcast_tables + ", ")
                    .append("ParameterMappings=" + super.toString() + "]");
            return (sb.toString());
        }
    }; // END CLASS

    /**
     * Set<Integer> pool used by calculatePartitionsForCache
     */
    private final ObjectPool partitionSetPool = new StackObjectPool(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new HashSet<Integer>());
        }

        public void passivateObject(Object obj) throws Exception {
            @SuppressWarnings("unchecked")
            Set<Integer> set = (Set<Integer>) obj;
            set.clear();
        };
    }, 1000);

    /**
     * Set<Integer>[4] pool used by calculatePartitionsForCache
     */
    private final ObjectPool mcPartitionSetPool = new StackObjectPool(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new HashSet[] { new HashSet<Integer>(), new HashSet<Integer>(), new HashSet<Integer>(), new HashSet<Integer>() });
        }

        public void passivateObject(Object obj) throws Exception {
            @SuppressWarnings("unchecked")
            HashSet<Integer> sets[] = (HashSet<Integer>[]) obj;
            for (HashSet<Integer> s : sets) {
                s.clear();
            } // FOR

        };
    }, 1000);

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Convenience constructor that uses DefaultHasher
     */
    public PartitionEstimator(Database catalog_db) {
        this(catalog_db, new DefaultHasher(catalog_db, CatalogUtil.getNumberOfPartitions(catalog_db)));
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public PartitionEstimator(Database catalog_db, AbstractHasher hasher) {
        this.catalog_db = catalog_db;
        this.hasher = hasher;
        this.initCatalog(catalog_db);
        
        if (trace.get())
            LOG.trace("Created a new PartitionEstimator with a " + hasher.getClass() + " hasher!");
    }

    // ----------------------------------------------------------------------------
    // BASE DATA MEMBERS METHODS
    // ----------------------------------------------------------------------------

    public Database getDatabase() {
        return catalog_db;
    }

    /**
     * Return the hasher used in this estimator instance
     * 
     * @return
     */
    public AbstractHasher getHasher() {
        return (this.hasher);
    }

    /**
     * Initialize a new catalog for this PartitionEstimator
     * 
     * @param new_catalog_db
     */
    public void initCatalog(Database new_catalog_db) {
        // Check whether any of our cache partition columns have changed
        // In which cache we know need to invalidate our cache entries
        /*
         * if (this.catalog_db != null) { for (Table catalog_tbl :
         * this.catalog_db.getTables()) { String table_key =
         * CatalogKey.createKey(catalog_tbl); Table new_catalog_tbl =
         * new_catalog_db.getTables().get(catalog_tbl.getName()); // This table
         * is not in our new catalog or it doesn't have a partitioning column if
         * (new_catalog_tbl == null || new_catalog_tbl.getPartitioncolumn() ==
         * null) { LOG.debug("Removing partitioning information for " +
         * catalog_tbl); if
         * (this.table_partition_columns.containsKey(table_key))
         * this.invalidateTableCache(table_key);
         * this.table_partition_columns.remove(table_key); continue; } //
         * Otherwise we will always just update our cache // Invalidate the
         * table's cache if the partitioning column has changed String
         * new_partition_key =
         * CatalogKey.createKey(new_catalog_tbl.getPartitioncolumn()); String
         * orig_partition_key = this.table_partition_columns.get(table_key); if
         * (orig_partition_key != null &&
         * !new_partition_key.equals(orig_partition_key)) {
         * this.invalidateTableCache(table_key); }
         * this.table_partition_columns.put(table_key, new_partition_key); } //
         * FOR }
         */
        this.catalog_db = new_catalog_db;
        this.hasher.init(new_catalog_db);
        this.clear();
        this.buildCatalogCache();
    }

    private synchronized void buildCatalogCache() {
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc() == false && catalog_proc.getParameters().size() > 0) {
                ProcParameter catalog_param = null;
                int param_idx = catalog_proc.getPartitionparameter();
                if (param_idx == NullProcParameter.PARAM_IDX || catalog_proc.getParameters().isEmpty()) {
                    catalog_param = NullProcParameter.singleton(catalog_proc);
                } else if (param_idx == RandomProcParameter.PARAM_IDX) {
                    catalog_param = RandomProcParameter.singleton(catalog_proc);
                } else {
                    catalog_param = catalog_proc.getParameters().get(param_idx);
                }
                this.cache_procPartitionParameters.put(catalog_proc, catalog_param);
                if (debug.get())
                    LOG.debug(catalog_proc + " ProcParameter Cache: " + (catalog_param != null ? catalog_param.fullName() : catalog_param));
            }
        } // FOR

        for (Table catalog_tbl : this.catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            Column catalog_col = catalog_tbl.getPartitioncolumn();
            if (catalog_col instanceof VerticalPartitionColumn) {
                catalog_col = ((VerticalPartitionColumn) catalog_col).getHorizontalColumn();
                assert ((catalog_col instanceof VerticalPartitionColumn) == false) : catalog_col;
            }
            this.cache_tablePartitionColumns.put(catalog_tbl, catalog_col);
            if (debug.get())
                LOG.debug(String.format("%s Partition Column Cache: %s", catalog_tbl.getName(), catalog_col));
        } // FOR
        for (CacheEntry entry : this.cache_fragmentEntries.values()) {
            entry.cache_valid = false;
        }
        for (CacheEntry entry : this.cache_statementEntries.values()) {
            entry.cache_valid = false;
        }

        // Generate a list of all the partition ids, so that we can quickly
        // add them to the output when estimating later on
        if (this.all_partitions.size() != this.hasher.getNumPartitions()) {
            this.all_partitions.clear();
            this.all_partitions.addAll(CatalogUtil.getAllPartitionIds(this.catalog_db));
            this.num_partitions = this.all_partitions.size();
            assert (this.hasher.getNumPartitions() == this.num_partitions);
            if (debug.get())
                LOG.debug(String.format("Initialized PartitionEstimator with %d partitions using the %s hasher", this.num_partitions, this.hasher.getClass().getSimpleName()));
        }
    }

    /**
     * Completely clear the PartitionEstimator's internal cache This should only
     * really be used for testing
     */
    public void clear() {
        this.cache_procPartitionParameters.clear();
        this.cache_tablePartitionColumns.clear();
        this.cache_fragmentEntries.clear();
        this.cache_statementEntries.clear();
        this.cache_stmtPartitionParameters.clear();
    }

    // ----------------------------------------------------------------------------
    // INTERNAL CACHE METHODS
    // ----------------------------------------------------------------------------

    /**
     * This is the method that actually picks the Statement apart and figures
     * out where the columns and parameters are used together
     * 
     * @param catalog_stmt
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private synchronized void generateCache(final Statement catalog_stmt) throws Exception {
        // Check whether we already have a CacheEntry for the Statement that we
        // can reuse
        String stmt_key = CatalogKey.createKey(catalog_stmt);
        QueryType stmt_type = QueryType.get(catalog_stmt.getQuerytype());
        PartitionEstimator.CacheEntry stmt_cache = this.cache_statementEntries.get(stmt_key);
        if (stmt_cache == null) {
            stmt_cache = new PartitionEstimator.CacheEntry(stmt_type);
        } else {
            // assert(stmt_cache.isValid()) :
            // "Unexpected invalid cache entry for " +
            // CatalogUtil.getDisplayName(catalog_stmt);
            stmt_cache.setValid();
        }
        Collection<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
        if (debug.get())
            LOG.debug("Generating partitioning cache for " + catalog_stmt);

        // Important: Work through the fragments in reverse so that we go from
        // the bottom of the tree up.
        // We are assuming that we can get the StmtParameter->Column mapping
        // that we need from either the
        // multi-partition plan or the single-partition plan and that the
        // mapping will be the same in both
        // cases. Therefore, we don't need to differentiate whether we are
        // picking apart one or the other,
        // nor do we need to switch to a different cache entry for the Statement
        // if we realize that we
        // are going to be single-partition or not.
        // We have to go through all of the fragments because we don't know
        // which set the system will
        // be calling at runtime.
        CatalogMap<?> fragment_sets[] = new CatalogMap<?>[] { catalog_stmt.getFragments(), catalog_stmt.getMs_fragments(), };
        for (int i = 0; i < fragment_sets.length; i++) {
            if (fragment_sets[i] == null || fragment_sets[i].isEmpty())
                continue;
            CatalogMap<PlanFragment> fragments = (CatalogMap<PlanFragment>) fragment_sets[i];
            boolean singlesited = (i == 0);
            if (trace.get())
                LOG.trace("Analyzing " + fragments.size() + " " + (singlesited ? "single" : "multi") + "-sited fragments for " + catalog_stmt.fullName());

            // Examine each fragment and pick apart how the tables are
            // referenced
            // The order doesn't matter here
            for (PlanFragment catalog_frag : fragments) {
                // Again, always check whether we already have a CacheEntry for
                // the PlanFragment that we can reuse
                String frag_key = CatalogKey.createKey(catalog_frag);
                PartitionEstimator.CacheEntry frag_cache = this.cache_fragmentEntries.get(frag_key);
                if (frag_cache == null) {
                    frag_cache = new PartitionEstimator.CacheEntry(stmt_type);
                } else if (frag_cache.isValid()) {
                    assert (!frag_cache.isValid()) : "Cache entry for " + CatalogUtil.getDisplayName(catalog_frag) + " is marked as valid when we were expecting to be invalid\n" + this.toString();
                    frag_cache.setValid();
                }

                AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
                Collection<Table> frag_tables = CatalogUtil.getReferencedTablesForTree(catalog_db, root);
                Table tables_arr[] = new Table[frag_tables.size()];
                tables_arr = frag_tables.toArray(tables_arr);
                assert (tables_arr.length == frag_tables.size());
                if (trace.get())
                    LOG.trace("Analyzing fragment #" + catalog_frag);

                // Check whether the predicate expression in this PlanFragment
                // contains an OR
                // We need to know this if we get hit with Multi-Column
                // Partitioning
                // XXX: Why does this matter??
                Collection<ExpressionType> exp_types = PlanNodeUtil.getScanExpressionTypes(catalog_db, root);
                if (exp_types.contains(ExpressionType.CONJUNCTION_OR)) {
                    if (debug.get())
                        LOG.warn(CatalogUtil.getDisplayName(catalog_frag) + " contains OR conjunction. Cannot be used with multi-column partitioning");
                    stmt_cache.markContainsOR(true);
                    frag_cache.markContainsOR(true);
                }

                // If there are no tables, then we need to double check that the
                // "non-transactional" flag
                // is set for the fragment. This means that this fragment does
                // not operate directly on
                // a persistent table in the database.
                // We'll add an entry in the cache using our special "no tables"
                // flag. This means that
                // the fragment needs to be executed locally.
                if (frag_tables.isEmpty()) {
                    String msg = catalog_frag + " in " + catalog_stmt.fullName() + " does not reference any tables";
                    if (!catalog_frag.getNontransactional()) {
                        LOG.warn(catalog_stmt.fullName() + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false)));
                        for (PlanFragment f : fragments) {
                            LOG.warn("singlePartiton=" + singlesited + " - " + f.fullName() + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getPlanNodeTreeForPlanFragment(f)));
                        }
                        throw new Exception(msg + " but the non-transactional flag is not set");
                    }
                    if (trace.get())
                        LOG.trace(msg);
                }
                if (trace.get())
                    LOG.trace("Fragment Tables: " + frag_tables);

                // We only need to find where the partition column is referenced
                // If it's not in there, then this query has to be broadcasted
                // to all nodes
                // Note that we pass all the tables that are part of the
                // fragment, since we need to be able to handle joins
                ColumnSet cset = CatalogUtil.extractFragmentColumnSet(catalog_frag, false, tables_arr);
                assert (cset != null);
                Map<Column, Set<Column>> column_joins = new TreeMap<Column, Set<Column>>();
                if (trace.get())
                    LOG.trace("Extracted Column Set for " + Arrays.toString(tables_arr) + ":\n" + cset.debug());

                // If there are no columns, then this fragment is doing a full
                // table scan
                if (cset.isEmpty() && tables_arr.length > 0) {
                    if (trace.get())
                        LOG.trace("No columns accessed in " + catalog_frag + " despite reading " + tables_arr.length + " tables");
                    stmt_cache.markAsBroadcast(tables_arr);
                    frag_cache.markAsBroadcast(tables_arr);

                // Fragment references the columns for our tables. Pick them
                // apart!
                } else {
                    // First go through all the entries and add any mappings
                    // from
                    // Columns to StmtParameters to our stmt_cache
                    for (ColumnSet.Entry entry : cset) {
                        if (trace.get())
                            LOG.trace("Examining extracted ColumnSetEntry: " + entry);

                        // Column = Column
                        if (entry.getFirst() instanceof Column && entry.getSecond() instanceof Column) {
                            Column col0 = (Column) entry.getFirst();
                            Column col1 = (Column) entry.getSecond();

                            if (!entry.getComparisonExp().equals(ExpressionType.COMPARE_EQUAL)) {
                                LOG.warn("Unsupported non-equality join in " + catalog_stmt + ": " + entry);
                            } else {
                                if (!column_joins.containsKey(col0))
                                    column_joins.put(col0, new TreeSet<Column>());
                                if (!column_joins.containsKey(col1))
                                    column_joins.put(col1, new TreeSet<Column>());
                                column_joins.get(col0).add(col1);
                                column_joins.get(col1).add(col0);
                            }
                            continue;
                        }

                        // Look for predicates with StmtParameters
                        for (Table catalog_tbl : frag_tables) {
                            Column catalog_col = null;
                            StmtParameter catalog_param = null;
                            // if (trace.get()) {
                            // LOG.trace("Current Table: " +
                            // catalog_tbl.hashCode());
                            //
                            // if (entry.getFirst() != null) {
                            // LOG.trace("entry.getFirst().getParent(): " +
                            // (entry.getFirst().getParent() != null ?
                            // entry.getFirst().getParent().hashCode() :
                            // entry.getFirst() + " parent is null?"));
                            //
                            // if (entry.getFirst().getParent() instanceof
                            // Table) {
                            // Table parent = entry.getFirst().getParent();
                            // if
                            // (parent.getName().equals(catalog_tbl.getName()))
                            // {
                            // assert(parent.equals(catalog_tbl)) :
                            // "Mismatch on " + parent.getName() + "???";
                            // }
                            // }
                            //
                            // } else {
                            // LOG.trace("entry.getFirst():             " +
                            // null);
                            // }
                            // if (entry.getSecond() != null) {
                            // LOG.trace("entry.getSecond().getParent(): " +
                            // (entry.getSecond().getParent() != null ?
                            // entry.getSecond().getParent().hashCode() :
                            // entry.getSecond() + " parent is null?"));
                            // } else {
                            // LOG.trace("entry.getSecond():             " +
                            // null);
                            // }
                            // }

                            // Column = StmtParameter
                            if (entry.getFirst().getParent() != null && entry.getFirst().getParent().equals(catalog_tbl) && entry.getSecond() instanceof StmtParameter) {
                                catalog_col = (Column) entry.getFirst();
                                catalog_param = (StmtParameter) entry.getSecond();
                                // StmtParameter = Column
                            } else if (entry.getSecond().getParent() != null && entry.getSecond().getParent().equals(catalog_tbl) && entry.getFirst() instanceof StmtParameter) {
                                catalog_col = (Column) entry.getSecond();
                                catalog_param = (StmtParameter) entry.getFirst();
                            }
                            if (catalog_col != null && catalog_param != null) {
                                if (trace.get())
                                    LOG.trace("[" + CatalogUtil.getDisplayName(catalog_tbl) + "] Adding cache entry for " + CatalogUtil.getDisplayName(catalog_frag) + ": " + entry);
                                stmt_cache.put(catalog_col, catalog_param.getIndex(), catalog_tbl);
                                frag_cache.put(catalog_col, catalog_param.getIndex(), catalog_tbl);
                            }
                        } // FOR (tables)
                        if (trace.get())
                            LOG.trace("-------------------");
                    } // FOR (entry)

                    // We now have to take a second pass through the column
                    // mappings
                    // This will pick-up those columns that are joined together
                    // where one of them is also referenced
                    // with an input parameter. So we will map the input
                    // parameter to the second column as well
                    PartitionEstimator.populateColumnJoins(column_joins);

                    for (Column catalog_col : column_joins.keySet()) {
                        // Otherwise, we have to examine the the ColumnSet and
                        // look for any reference to this column
                        if (trace.get())
                            LOG.trace("Trying to find all references to " + CatalogUtil.getDisplayName(catalog_col));
                        for (Column other_col : column_joins.get(catalog_col)) {
                            // IMPORTANT: If the other entry is a column from
                            // another table and we don't
                            // have a reference in stmt_cache for ourselves,
                            // then we can look
                            // to see if this guy was used against a
                            // StmtParameter some where else in the Statement
                            // If this is the case, then we can substitute that
                            // mofo in it's place
                            if (stmt_cache.containsKey(catalog_col)) {
                                for (Integer param_idx : stmt_cache.get(catalog_col)) {
                                    if (trace.get())
                                        LOG.trace("Linking " + CatalogUtil.getDisplayName(other_col) + " to parameter #" + param_idx + " because of " + catalog_col.fullName());
                                    stmt_cache.put(other_col, param_idx, (Table) other_col.getParent());
                                    frag_cache.put(other_col, param_idx, (Table) other_col.getParent());
                                } // FOR (StmtParameter.Index)
                            }
                        } // FOR (Column)
                    } // FOR (Column)
                }
                if (trace.get())
                    LOG.trace(frag_cache);

                // Loop through all of our tables and make sure that there is an
                // entry in the PlanFragment CacheEntrry
                // If there isn't, then that means there was no predicate on the
                // table and therefore the PlanFragment
                // must be broadcast to all partitions (unless it is replicated)
                for (Table catalog_tbl : tables_arr) {
                    if (!frag_cache.hasTable(catalog_tbl)) {
                        if (trace.get())
                            LOG.trace("No column predicate for " + CatalogUtil.getDisplayName(catalog_tbl) + ". " + "Marking as broadcast for " + CatalogUtil.getDisplayName(catalog_frag) + ": "
                                    + frag_cache.getTables());
                        frag_cache.markAsBroadcast(catalog_tbl);
                        stmt_cache.markAsBroadcast(catalog_tbl);
                    }
                } // FOR

                // Store the Fragment cache and update the Table xref mapping
                this.cache_fragmentEntries.put(frag_key, frag_cache);
                this.addTableCacheXref(frag_cache, frag_tables);
            } // FOR (fragment)

            // Then for updates we need to look to see whether they are updating
            // an attribute that they
            // are partitioned on. If so, then it gets dicey because we need to
            // know the value...
            if (stmt_type == QueryType.UPDATE) {
                List<Table> tables = new ArrayList<Table>();
                ColumnSet update_cset = new ColumnSet();
                for (Table catalog_tbl : CatalogUtil.getReferencedTables(catalog_stmt)) {
                    update_cset.clear();
                    tables.clear();
                    tables.add(catalog_tbl);
                    AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
                    CatalogUtil.extractUpdateColumnSet(catalog_stmt, catalog_db, update_cset, root_node, true, tables);

                    boolean found = false;
                    for (ColumnSet.Entry entry : update_cset) {
                        Column catalog_col = null;
                        StmtParameter catalog_param = null;

                        // For now we only care up look-ups using parameters
                        if (entry.getFirst() instanceof StmtParameter) {
                            catalog_param = (StmtParameter) entry.getFirst();
                            catalog_col = (Column) entry.getSecond();
                        } else if (entry.getSecond() instanceof StmtParameter) {
                            catalog_param = (StmtParameter) entry.getSecond();
                            catalog_col = (Column) entry.getFirst();
                        } else {
                            if (trace.get())
                                LOG.trace("Skipping entry " + entry + " when examing the update information for " + catalog_tbl);
                            continue;
                        }
                        assert (catalog_col != null);
                        assert (catalog_param != null);
                        stmt_cache.put(catalog_col, catalog_param.getIndex(), catalog_tbl);
                        found = true;
                    } // FOR
                    if (found && trace.get())
                        LOG.trace("UpdatePlanNode in " + catalog_stmt.fullName() + " modifies " + catalog_tbl);
                } // FOR
            } // IF (UPDATE)
        } // FOR (single-partition vs multi-partition)

        // Add the Statement cache entry and update the Table xref map
        this.cache_statementEntries.put(stmt_key, stmt_cache);
        this.addTableCacheXref(stmt_cache, stmt_tables);
    }

    /**
     * Update the cache entry xref mapping for a set of tables
     * 
     * @param entry
     * @param tables
     */
    private void addTableCacheXref(CacheEntry entry, Collection<Table> tables) {
        for (Table catalog_tbl : tables) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (!this.table_cache_xref.containsKey(table_key)) {
                this.table_cache_xref.put(table_key, new HashSet<CacheEntry>());
            }
            this.table_cache_xref.get(table_key).add(entry);
        } // FOR
    }

    // ----------------------------------------------------------------------------
    // TABLE ROW METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the partition for the given VoltTableRow
     * 
     * @param catalog_tbl
     * @param row
     * @return
     * @throws Exception
     */
    public int getTableRowPartition(final Table catalog_tbl, final VoltTableRow row) throws Exception {
        assert (!catalog_tbl.getIsreplicated()) : "Trying to partition replicated table: " + catalog_tbl;
        if (debug.get())
            LOG.debug("Calculating partition for VoltTableRow from " + catalog_tbl);

        int partition = -1;
        Column catalog_col = this.cache_tablePartitionColumns.get(catalog_tbl);
        assert (catalog_col != null) : "Null partition column: " + catalog_tbl;
        assert ((catalog_col instanceof VerticalPartitionColumn) == false) : "Invalid partitioning column: " + catalog_col.fullName();

        // Multi-Column Partitioning
        if (catalog_col instanceof MultiColumn) {
            MultiColumn mc = (MultiColumn) catalog_col;
            if (debug.get())
                LOG.debug(catalog_tbl.getName() + " MultiColumn: " + mc);

            Object values[] = new Object[mc.size()];
            for (int i = 0; i < values.length; i++) {
                Column inner = mc.get(i);
                VoltType type = VoltType.get(inner.getType());
                values[i] = row.get(inner.getIndex(), type);
            } // FOR
            partition = this.hasher.multiValueHash(values);

            // Single-Column Partitioning
        } else {
            VoltType type = VoltType.get(catalog_col.getType());
            Object value = row.get(catalog_col.getIndex(), type);
            partition = this.hasher.hash(value, catalog_col);
            if (debug.get())
                LOG.debug(String.format("%s SingleColumn: Value=%s / Partition=%d", catalog_col.fullName(), value, partition));
        }
        assert (partition >= 0) : "Invalid partition for " + catalog_tbl;
        return (partition);
    }

    // ----------------------------------------------------------------------------
    // BASE PARTITION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the target partition for a StoredProcedureInvocation instance
     * 
     * @param invocation
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(StoredProcedureInvocation invocation) throws Exception {
        Procedure catalog_proc = this.catalog_db.getProcedures().get(invocation.getProcName());
        if (catalog_proc == null) {
            catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(invocation.getProcName());
        }
        assert(catalog_proc != null) :
            "Invalid procedure name '" + invocation.getProcName() + "'";
        return (this.getBasePartition(catalog_proc, invocation.getParams().toArray(), false));
    }

    /**
     * Returns the target partition for a stored procedure + parameters
     * 
     * @param catalog_proc
     * @param params
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(Procedure catalog_proc, Object params[]) throws Exception {
        return (this.getBasePartition(catalog_proc, params, false));
    }

    /**
     * Return the target partition for a TransactionTrace
     * 
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(final TransactionTrace txn_trace) throws Exception {
        if (debug.get())
            LOG.debug("Calculating base partition for " + txn_trace.toString());
        return (this.getBasePartition(txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams(), true));
    }

    /**
     * Main method for calculating the base partition for a stored procedure
     * 
     * @param catalog_proc
     * @param params
     * @param force
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(final Procedure catalog_proc, Object params[], boolean force) throws Exception {
        assert (catalog_proc != null);
        assert (params != null);
        ProcParameter catalog_param = this.cache_procPartitionParameters.get(catalog_proc);

        if (catalog_param == null && force) {
            if (force) {
                int idx = catalog_proc.getPartitionparameter();
                if (idx == NullProcParameter.PARAM_IDX || catalog_proc.getParameters().isEmpty()) {
                    catalog_param = NullProcParameter.singleton(catalog_proc);
                } else if (idx == RandomProcParameter.PARAM_IDX) {
                    catalog_param = RandomProcParameter.singleton(catalog_proc);
                } else {
                    catalog_param = catalog_proc.getParameters().get(idx);
                }
                this.cache_procPartitionParameters.put(catalog_proc, catalog_param);
                if (debug.get())
                    LOG.debug("Added cached " + catalog_param + " for " + catalog_proc);
            } else {
                if (debug.get())
                    LOG.debug(catalog_proc + " has no parameters. No base partition for you!");
                return (null);
            }
        }

        if (force == false && (catalog_param == null || catalog_param instanceof NullProcParameter)) {
            if (debug.get())
                LOG.debug(catalog_proc + " does not have a pre-defined partition parameter. No base partition!");
            return (null);
            // } else if (!force && !catalog_proc.getSinglepartition()) {
            // if (debug.get()) LOG.debug(catalog_proc +
            // " is not marked as single-partitioned. Executing as multi-partition");
            // return (null);
        }
        Integer partition = null;
        boolean is_array = catalog_param.getIsarray();

        // Special Case: RandomProcParameter
        if (catalog_param instanceof RandomProcParameter) {
            partition = RandomProcParameter.rand.nextInt(this.num_partitions);
        }
        // Special Case: MultiProcParameter
        else if (catalog_param instanceof MultiProcParameter) {
            MultiProcParameter mpp = (MultiProcParameter) catalog_param;
            if (debug.get())
                LOG.debug(catalog_proc.getName() + " MultiProcParameter: " + mpp);
            int hashes[] = new int[mpp.size()];
            for (int i = 0; i < hashes.length; i++) {
                int mpp_param_idx = mpp.get(i).getIndex();
                assert (mpp_param_idx >= 0) : "Invalid Partitioning MultiProcParameter #" + mpp_param_idx;
                assert (mpp_param_idx < params.length) : CatalogUtil.getDisplayName(mpp) + " < " + params.length;
                Integer hash = this.calculatePartition(catalog_proc, params[mpp_param_idx], is_array);
                if (hash == null)
                    hash = 0;
                hashes[i] = hash.intValue();
                if (debug.get())
                    LOG.debug(mpp.get(i) + " value[" + params[mpp_param_idx] + "] => hash[" + hashes[i] + "]");
            } // FOR
            partition = this.hasher.multiValueHash(hashes);
            if (debug.get())
                LOG.debug(Arrays.toString(hashes) + " => " + partition);
            // Single ProcParameter
        } else {
            if (debug.get())
                LOG.debug("Calculating base partition using " + catalog_param.fullName() + ": " + params[catalog_param.getIndex()]);
            // try {
            partition = this.calculatePartition(catalog_proc, params[catalog_param.getIndex()], is_array);
            // } catch (NullPointerException ex) {
            // LOG.error("catalog_proc = " + catalog_proc);
            // LOG.error("catalog_param = " + CatalogUtil.debug(catalog_param));
            // LOG.error("params = " + Arrays.toString(params));
            // LOG.error("is_array = " + is_array);
            // LOG.error("Busted!", ex);
            // System.exit(1);
            // }
        }
        return (partition);
    }

    // ----------------------------------------------------------------------------
    // DETAILED PARTITON METHODS
    // ----------------------------------------------------------------------------

    /**
     * @param xact
     * @return
     * @throws Exception
     */
    public Set<Integer> getAllPartitions(final TransactionTrace xact) throws Exception {
        Set<Integer> partitions = new HashSet<Integer>();
        int base_partition = this.getBasePartition(xact.getCatalogItem(this.catalog_db), xact.getParams(), true);
        partitions.add(base_partition);

        Set<Integer> temp = new HashSet<Integer>();
        for (QueryTrace query : xact.getQueries()) {
            partitions.addAll(this.getAllPartitions(temp, query.getCatalogItem(this.catalog_db), query.getParams(), base_partition));
            temp.clear();
        } // FOR

        return (partitions);
    }

    /**
     * Return the list of partitions that this QueryTrace object will touch
     * 
     * @param query
     * @return
     */
    public Set<Integer> getAllPartitions(final QueryTrace query, Integer base_partition) throws Exception {
        return (this.getAllPartitions(query.getCatalogItem(this.catalog_db), query.getParams(), base_partition));
    }

    /**
     * Return the table -> partitions mapping for the given QueryTrace object
     * 
     * @param query
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, Set<Integer>> getTablePartitions(final QueryTrace query, Integer base_partition) throws Exception {
        return (this.getTablePartitions(query.getCatalogItem(this.catalog_db), query.getParams(), base_partition));
    }

    /**
     * @param catalog_stmt
     * @param params
     * @return
     */
    public Set<Integer> getAllPartitions(final Statement catalog_stmt, Object params[], int base_partition) throws Exception {
        return (this.getAllPartitions(new HashSet<Integer>(), catalog_stmt, params, base_partition));
    }

    /**
     * @param partitions
     * @param catalog_stmt
     * @param params
     * @param base_partition
     * @throws Exception
     */
    public Set<Integer> getAllPartitions(final Set<Integer> partitions, final Statement catalog_stmt, final Object params[], final int base_partition) throws Exception {
        Set<Integer> all_partitions = new HashSet<Integer>();

        // Note that we will use the single-sited fragments (if available) since
        // they will be
        // faster for us to figure out what partitions has the data that this
        // statement needs
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        this.getAllFragmentPartitions(null, all_partitions, fragments.values(), params, base_partition);
        return (all_partitions);
    }

    // ----------------------------------------------------------------------------
    // STATEMENT PARTITION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return all of the partitions per table for the given Statement object
     * 
     * @param catalog_stmt
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, Set<Integer>> getTablePartitions(final Statement catalog_stmt, Object params[], Integer base_partition) throws Exception {
        Map<String, Set<Integer>> all_partitions = new HashMap<String, Set<Integer>>();
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        for (PlanFragment catalog_frag : fragments) {
            try {
                Map<String, Set<Integer>> frag_partitions = new HashMap<String, Set<Integer>>();
                this.calculatePartitionsForFragment(frag_partitions, null, catalog_frag, params, base_partition);
                for (String table_key : frag_partitions.keySet()) {
                    if (!all_partitions.containsKey(table_key)) {
                        all_partitions.put(table_key, frag_partitions.get(table_key));
                    } else {
                        all_partitions.get(table_key).addAll(frag_partitions.get(table_key));
                    }
                } // FOR
            } catch (Throwable ex) {
                throw new Exception("Failed to calculate table partitions for " + catalog_frag.fullName(), ex);
            }
        } // FOR
        return (all_partitions);
    }

    // ----------------------------------------------------------------------------
    // FRAGMENT PARTITON METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the set of StmtParameter offsets that can be used to figure out
     * what partitions the Statement invocation will touch. This is used to
     * quickly figure out whether that invocation is single-partition or not. If
     * this Statement will always be multi-partition, or if the tables it
     * references uses a MultiColumn partitioning attrtibute, then the return
     * set will be null. This is at a coarse-grained level. You still need to
     * use the other PartitionEstimator methods to figure out where to send
     * PlanFragments.
     * 
     * @param catalog_stmt
     * @return
     */
    public Collection<Integer> getStatementEstimationParameters(Statement catalog_stmt) {
        Collection<Integer> all_param_idxs = this.cache_stmtPartitionParameters.get(catalog_stmt);
        if (all_param_idxs == null) {
            all_param_idxs = new HashSet<Integer>();

            // Assume single-partition
            if (catalog_stmt.getHas_singlesited() == false) {
                if (debug.get())
                    LOG.warn("There is no single-partition query plan for " + catalog_stmt.fullName());
                return (null);
            }

            for (PlanFragment catalog_frag : catalog_stmt.getFragments()) {
                PartitionEstimator.CacheEntry cache_entry = null;
                try {
                    cache_entry = this.getFragmentCacheEntry(catalog_frag);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve CacheEntry for " + catalog_frag.fullName());
                }

                // If this PlanFragment has a broadcast, then this statment
                // can't be used for fast look-ups
                if (cache_entry.hasBroadcast()) {
                    if (debug.get())
                        LOG.warn(String.format("%s contains an operation that must be broadcast. Cannot be used for fast look-ups", catalog_frag.fullName()));
                    return (null);
                }

                for (Table catalog_tbl : cache_entry.getTables()) {
                    Column partition_col = catalog_tbl.getPartitioncolumn();
                    if (partition_col instanceof MultiColumn) {
                        if (debug.get())
                            LOG.warn(String.format("%s references %s, which is partitioned on %s. Cannot be used for fast look-ups", catalog_frag.fullName(), catalog_tbl.getName(),
                                    partition_col.fullName()));
                        return (null);
                    } else if (partition_col != null && cache_entry.containsKey(partition_col)) {
                        for (int idx : cache_entry.get(partition_col)) {
                            all_param_idxs.add(Integer.valueOf(idx));
                        } // FOR
                    }
                } // FOR
            } // FOR
            this.cache_stmtPartitionParameters.put(catalog_stmt, all_param_idxs);
        }
        return (all_param_idxs);
    }

    /**
     * @param frag_partitions
     * @param fragments
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<PlanFragment, Set<Integer>> getAllFragmentPartitions(final Map<PlanFragment, Set<Integer>> frag_partitions, final PlanFragment fragments[], final Object params[],
            final Integer base_partition) throws Exception {
        this.getAllFragmentPartitions(frag_partitions, null, fragments, params, base_partition);
        return (frag_partitions);
    }

    /**
     * Populate a mapping from PlanFragments to sets of PartitionIds NOTE: This
     * is the one to use at runtime in the BatchPlanner because it doesn't
     * allocate any new Collections!
     * 
     * @param frag_partitions
     * @param frag_all_partitions
     * @param fragments
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public void getAllFragmentPartitions(final Map<PlanFragment, Set<Integer>> frag_partitions, final Set<Integer> frag_all_partitions, PlanFragment fragments[], final Object params[],
            final Integer base_partition) throws Exception {
        // Loop through this Statement's plan fragments and get the partitions
        for (PlanFragment catalog_frag : fragments) {
            Set<Integer> partitions = null;

            // If we have a FragPartion map, then use an entry from that
            if (frag_partitions != null) {
                partitions = frag_partitions.get(catalog_frag);
                if (partitions == null) {
                    partitions = new HashSet<Integer>();
                    frag_partitions.put(catalog_frag, partitions);
                } else {
                    partitions.clear();
                }
                // Otherwise use our AllPartitions set
            } else {
                partitions = frag_all_partitions;
            }
            assert (partitions != null);

            this.calculatePartitionsForFragment(null, partitions, catalog_frag, params, base_partition);

            // If there were no partitions, then the PlanFragment needs to be
            // execute on the base partition
            // Because these are the PlanFragments that aggregate the results
            // together
            // XXX: Not sure if this is right, but it's 5:30pm on a snowy night
            // so it's good enough for me...
            if (partitions.isEmpty())
                partitions.add(base_partition);

            if (frag_partitions != null && frag_all_partitions != null)
                frag_all_partitions.addAll(partitions);
        } // FOR
    }

    /**
     * Return the list partitions that this fragment needs to be sent to based
     * on the parameters
     * 
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Set<Integer> getPartitions(final PlanFragment catalog_frag, Object params[], Integer base_partition) throws Exception {
        Set<Integer> partitions = new HashSet<Integer>();
        this.calculatePartitionsForFragment(null, partitions, catalog_frag, params, base_partition);
        return (partitions);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL CALCULATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    private void calculatePartitionsForFragment(final Map<String, Set<Integer>> entry_partitions, final Set<Integer> all_partitions, PlanFragment catalog_frag, Object params[], Integer base_partition)
            throws Exception {
        if (trace.get())
            LOG.trace("Estimating partitions for PlanFragment #" + catalog_frag.fullName());
        PartitionEstimator.CacheEntry cache_entry = this.getFragmentCacheEntry(catalog_frag);
        this.calculatePartitionsForCache(entry_partitions, all_partitions, cache_entry, params, base_partition);
        if (debug.get()) {
            if (entry_partitions != null)
                LOG.debug(String.format("%s Table Partitions: %s", catalog_frag.fullName(), entry_partitions));
            if (all_partitions != null)
                LOG.debug(String.format("%s All Partitions: %s", catalog_frag.fullName(), all_partitions));
        }
        return;
    }

    private PartitionEstimator.CacheEntry getFragmentCacheEntry(PlanFragment catalog_frag) throws Exception {
        String frag_key = CatalogKey.createKey(catalog_frag);
        // Check whether we have generate the cache entries for this Statement
        // The CacheEntry object just tells us what input parameter to use for
        // hashing
        // to figure out where we need to go for each table.
        PartitionEstimator.CacheEntry cache_entry = this.cache_fragmentEntries.get(frag_key);
        if (cache_entry == null) {
            synchronized (this) {
                cache_entry = this.cache_fragmentEntries.get(frag_key);
                if (cache_entry == null) {
                    Statement catalog_stmt = (Statement) catalog_frag.getParent();
                    this.generateCache(catalog_stmt);
                    cache_entry = this.cache_fragmentEntries.get(frag_key);
                }
            } // SYNCHRONIZED
        }
        assert (cache_entry != null) : "Failed to retrieve CacheEntry for " + catalog_frag.fullName();
        return (cache_entry);
    }

    /**
     * @param cache_entry
     * @param params
     * @param base_partition
     * @return
     */
    private void calculatePartitionsForCache(final Map<String, Set<Integer>> entry_table_partitions,
                                             final Collection<Integer> entry_all_partitions,
                                             final PartitionEstimator.CacheEntry cache_entry,
                                             final Object params[],
                                             final Integer base_partition) throws Exception {

        // Hash the input parameters to determine what partitions we're headed to
        QueryType stmt_type = cache_entry.query_type;

        // Update cache
        if (cache_entry.is_array == null) {
            cache_entry.is_array = new boolean[params.length];
            for (int i = 0; i < cache_entry.is_array.length; i++) {
                cache_entry.is_array[i] = ClassUtil.isArray(params[i]);
            } // FOR
        }

        @SuppressWarnings("unchecked")
        final Set<Integer> table_partitions = (Set<Integer>) this.partitionSetPool.borrowObject();
        assert (table_partitions != null);
        table_partitions.clear();

        // Go through each table referenced in this CacheEntry and look-up the parameters that the 
        // partitioning columns are referenced against to determine what partitions we need to go to
        // IMPORTANT: If there are no tables (meaning it's some PlanFragment that combines data output
        // from other PlanFragments), then won't return anything because it is up to whoever
        // to figure out where to send this PlanFragment (it may be at the coordinator)
        Table tables[] = cache_entry.getTables();
        if (trace.get()) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put("CacheEntry", cache_entry);
            m.put("Tables", tables);
            m.put("Params", Arrays.toString(params));
            m.put("Base Partition", base_partition);
            LOG.trace("Calculating partitions for " + cache_entry.query_type + "\n" + StringUtil.formatMaps(m));
        }

        for (int table_idx = 0; table_idx < cache_entry.is_replicated.length; table_idx++) {
            final Table catalog_tbl = tables[table_idx];
            final boolean is_replicated = cache_entry.is_replicated[table_idx];

            // Easy Case: If this table is replicated and this query is a scan,
            // then we're in the clear and there's nothing else we need to do here
            // for the current table (but we still need to check the other guys).
            // Conversely, if it's replicated but we're performing an update or
            // a delete, then we know it's not single-sited.
            if (is_replicated) {
                if (stmt_type == QueryType.SELECT) {
                    if (trace.get())
                        LOG.trace("Cache entry " + cache_entry + " will execute on the local partition");
                    if (base_partition != null)
                        table_partitions.add(base_partition);
                } else if (stmt_type == QueryType.INSERT || stmt_type == QueryType.UPDATE || stmt_type == QueryType.DELETE) {
                    if (trace.get())
                        LOG.trace("Cache entry " + cache_entry + " must be broadcast to all partitions");
                    table_partitions.addAll(this.all_partitions);
                } else {
                    assert (false) : "Unexpected query type: " + stmt_type;
                }
            }
            // Otherwise calculate the partition value based on this table's partitioning column
            else {
                // Grab the parameter mapping for this column
                Column catalog_col = cache_tablePartitionColumns.get(catalog_tbl);
                if (trace.get())
                    LOG.trace("Partitioning Column: " + catalog_col.fullName());

                // Special Case: Multi-Column Partitioning
                // Strap on your seatbelts, we're going in!!!
                if (catalog_col instanceof MultiColumn) {
                    // HACK: All multi-column look-ups on queries with an OR
                    // must be broadcast
                    if (cache_entry.isMarkedContainsOR()) {
                        if (debug.get())
                            LOG.warn("Trying to use multi-column partitioning [" + catalog_col.fullName() + "] on query that contains an 'OR': " + cache_entry);
                        table_partitions.addAll(this.all_partitions);
                    } else {
                        MultiColumn mc = (MultiColumn) catalog_col;
                        @SuppressWarnings("unchecked")
                        HashSet<Integer> mc_partitions[] = (HashSet<Integer>[]) this.mcPartitionSetPool.borrowObject();

                        if (trace.get())
                            LOG.trace("Calculating columns for multi-partition colunmn: " + mc);
                        boolean is_valid = true;
                        for (int i = 0, mc_cnt = mc.size(); i < mc_cnt; i++) {
                            Column mc_column = mc.get(i);
                            // assert(cache_entry.get(mc_column_key) != null) :
                            // "Null CacheEntry: " + mc_column_key;
                            if (cache_entry.get(mc_column) != null) {
                                this.calculatePartitions(mc_partitions[i],
                                                         params,
                                                         cache_entry.is_array,
                                                         cache_entry.get(mc_column),
                                                         mc_column);
                            }

                            // Unless we have partition values for both keys,
                            // then it has to be a broadcast
                            if (mc_partitions[i].isEmpty()) {
                                if (debug.get())
                                    LOG.warn(String.format("No partitions for %s from %s. Cache entry %s must be broadcast to all partitions", mc_column.fullName(), mc.fullName(), cache_entry));
                                table_partitions.addAll(this.all_partitions);
                                is_valid = false;
                                break;
                            }
                            if (trace.get())
                                LOG.trace(CatalogUtil.getDisplayName(mc_column) + ": " + mc_partitions[i]);
                        } // FOR

                        // Now if we're here, then we have partitions for both
                        // of the columns and we're legit
                        // We therefore just need to take the cross product of
                        // the two sets and hash them together
                        if (is_valid) {
                            for (int part0 : mc_partitions[0]) {
                                for (int part1 : mc_partitions[1]) {
                                    int partition = this.hasher.multiValueHash(part0, part1);
                                    table_partitions.add(partition);
                                    if (trace.get())
                                        LOG.trace(String.format("MultiColumn Partitions[%d, %d] => %d", part0, part1, partition));
                                } // FOR
                            } // FOR
                        }
                        this.mcPartitionSetPool.returnObject(mc_partitions);
                    }
                } else {
                    int param_idxs[] = cache_entry.get(catalog_col);
                    if (trace.get())
                        LOG.trace("Param Indexes: " + param_idxs);

                    // Important: If there is no entry for this partitioning
                    // column, then we have to broadcast this mofo
                    if (param_idxs == null || param_idxs.length == 0) {
                        if (debug.get())
                            LOG.debug(String.format("No parameter mapping for %s. Fragment must be broadcast to all partitions", catalog_col.fullName()));
                        table_partitions.addAll(this.all_partitions);

                        // If there is nothing special, just shove off and have
                        // this method figure things out for us
                    } else {
                        if (trace.get())
                            LOG.trace("Calculating partitions normally for " + cache_entry);
                        this.calculatePartitions(table_partitions, params, cache_entry.is_array, param_idxs, catalog_col);
                    }
                }
            } // ELSE
            assert (table_partitions.size() <= this.num_partitions);

            if (entry_table_partitions != null) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                Set<Integer> table_p = entry_table_partitions.get(table_key);
                if (table_p == null) {
                    entry_table_partitions.put(table_key, new HashSet<Integer>(table_partitions));
                } else {
                    table_p.clear();
                    table_p.addAll(table_partitions);
                }
            }
            if (entry_all_partitions != null) {
                entry_all_partitions.addAll(table_partitions);
            }
            if (entry_table_partitions == null && entry_all_partitions.size() == this.num_partitions)
                break;
        } // FOR
        this.partitionSetPool.returnObject(table_partitions);
        return;
    }

    /**
     * Calculate the partitions touched for the given column
     * 
     * @param partitions
     * @param params
     * @param param_idxs
     * @param catalog_col
     */
    private Set<Integer> calculatePartitions(final Set<Integer> partitions, Object params[], boolean is_array[], int param_idxs[], Column catalog_col) {
        // Note that we have to go through all of the mappings from the
        // partitioning column
        // to parameters. This can occur when the partitioning column is
        // referenced multiple times
        for (int param_idx : param_idxs) {
            // IMPORTANT: Check if the parameter is an array. If it is, then we
            // have to
            // loop through and get the hash of all of the values
            if (is_array[param_idx]) {
                int num_elements = Array.getLength(params[param_idx]);
                if (trace.get())
                    LOG.trace("Parameter #" + param_idx + " is an array. Calculating multiple partitions...");
                for (int i = 0; i < num_elements; i++) {
                    Object value = Array.get(params[param_idx], i);
                    int partition_id = this.hasher.hash(value, catalog_col);
                    if (trace.get())
                        LOG.trace(CatalogUtil.getDisplayName(catalog_col) + " HASHING PARAM ARRAY[" + param_idx + "][" + i + "]: " + value + " -> " + partition_id);
                    partitions.add(partition_id);
                } // FOR
                // Primitive
            } else {
                int partition_id = this.hasher.hash(params[param_idx], catalog_col);
                if (trace.get())
                    LOG.trace(CatalogUtil.getDisplayName(catalog_col) + " HASHING PARAM[" + param_idx + "]: " + params[param_idx] + " -> " + partition_id);
                partitions.add(partition_id);
            }
        } // FOR
        return (partitions);
    }

    /**
     * Return the partition for a given procedure's parameter value
     * 
     * @param catalog_proc
     * @param partition_param_val
     * @return
     * @throws Exception
     */
    private Integer calculatePartition(Procedure catalog_proc, Object partition_param_val, boolean is_array) throws Exception {
        // If the parameter is an array, then just use the first value
        if (is_array) {
            int num_elements = Array.getLength(partition_param_val);
            if (num_elements == 0) {
                if (debug.get())
                    LOG.warn("Empty partitioning parameter array for " + catalog_proc);
                return (null);
            } else {
                partition_param_val = Array.get(partition_param_val, 0);
            }
        } else if (partition_param_val == null) {
            if (debug.get())
                LOG.warn("Null ProcParameter value: " + catalog_proc);
            return (null);
        }
        return (this.hasher.hash(partition_param_val, catalog_proc));
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Debug output
     */
    @Override
    public String toString() {
        String ret = "";
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            StringBuilder sb = new StringBuilder();
            boolean has_entries = false;
            sb.append(CatalogUtil.getDisplayName(catalog_proc)).append(":\n");
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                String stmt_key = CatalogKey.createKey(catalog_stmt);
                CacheEntry stmt_cache = this.cache_statementEntries.get(stmt_key);
                if (stmt_cache == null)
                    continue;
                has_entries = true;
                sb.append("  " + catalog_stmt.getName() + ": ").append(stmt_cache).append("\n");

                for (PlanFragment catalog_frag : CatalogUtil.getAllPlanFragments(catalog_stmt)) {
                    String frag_key = CatalogKey.createKey(catalog_frag);
                    CacheEntry frag_cache = this.cache_fragmentEntries.get(frag_key);
                    if (frag_cache == null)
                        continue;
                    sb.append("    PlanFragment[" + catalog_frag.getName() + "]: ").append(frag_cache).append("\n");
                }
            } // FOR
            if (has_entries)
                ret += sb.toString() + StringUtil.SINGLE_LINE;
        } // FOR
        return (ret);
    }

    /**
     * @param column_joins
     */
    protected static void populateColumnJoins(final Map<Column, Set<Column>> column_joins) {
        int orig_size = 0;
        for (Set<Column> cols : column_joins.values()) {
            orig_size += cols.size();
        }
        // First we have to take the Cartesian product of all mapped joins
        for (Column c0 : column_joins.keySet()) {
            // For each column that c0 is joined with, add a reference to c0 for
            // all the columns
            // that the other column references
            for (Column c1 : column_joins.get(c0)) {
                assert (!c1.equals(c0));
                for (Column c2 : column_joins.get(c1)) {
                    if (!c0.equals(c2))
                        column_joins.get(c2).add(c0);
                } // FOR
            } // FOR
        } // FOR

        int new_size = 0;
        for (Set<Column> cols : column_joins.values()) {
            new_size += cols.size();
        }
        if (new_size != orig_size)
            populateColumnJoins(column_joins);
    }

    /**
     * Generate a histogram of the base partitions used for all of the
     * transactions in the workload
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public Histogram<Integer> buildBasePartitionHistogram(Workload workload) throws Exception {
        final Histogram<Integer> h = new Histogram<Integer>();
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            int base_partition = this.getBasePartition(txn_trace);
            h.put(base_partition);
        } // FOR
        return (h);
    }

    /**
     * Pre-load the cache entries for all Statements
     * 
     * @param catalog_db
     */
    public void preload() {
        assert (this.catalog_db != null);
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                try {
                    this.generateCache(catalog_stmt);
                    this.getStatementEstimationParameters(catalog_stmt);
                } catch (Exception ex) {
                    LOG.fatal("Failed to generate cache for " + catalog_stmt.fullName(), ex);
                    System.exit(1);
                }
            } // FOR
        } // FOR

        for (CacheEntry entry : this.cache_fragmentEntries.values()) {
            entry.getTables();
        }
        for (CacheEntry entry : this.cache_statementEntries.values()) {
            entry.getTables();
        }

    }
}