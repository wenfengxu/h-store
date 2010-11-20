/**
 * 
 */
package edu.brown.benchmark;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.JarReader;

import edu.brown.benchmark.airline.AirlineProjectBuilder;
import edu.brown.benchmark.auctionmark.AuctionMarkProjectBuilder;
import edu.brown.benchmark.markov.MarkovProjectBuilder;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tpce.TPCEProjectBuilder;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 *
 */
public abstract class AbstractProjectBuilder extends VoltProjectBuilder {

    protected final Class<? extends AbstractProjectBuilder> base_class;
    protected final String project_name;
    protected final Class<?> procedures[];
    protected final Class<?> supplementals[];
    protected final String partitioning[][];
    
    protected final URL ddlURL;
    protected final URL ddlFkeysURL;
    
    protected final TransactionFrequencies txn_frequencies = new TransactionFrequencies();

    public static class TransactionFrequencies extends HashMap<Class<? extends VoltProcedure>, Integer> {
        private static final long serialVersionUID = 1L;
        
        public int getTotal() {
            int total = 0;
            for (Integer freq : this.values()) {
                assert(freq >= 0);
                total += freq;
            } // FOR
            return (total);
        }
    }
    
    
    /**
     * Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][]) {
        this(project_name, base_class, procedures, partitioning, new Class<?>[0], false);
    }
    
    /**
     * Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     * @param supplementals
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][], Class<?> supplementals[]) {
       this(project_name, base_class, procedures, partitioning, supplementals, false);
    }
    
    /**
     * Full Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     * @param supplementals
     * @param fkeys
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][], Class<?> supplementals[], boolean fkeys) {
        super();
        this.project_name = project_name;
        this.base_class = base_class;
        this.procedures = procedures;
        this.partitioning = partitioning;
        this.supplementals = supplementals;
        
        this.ddlFkeysURL = this.base_class.getResource(this.getDDLName(true));
        if (fkeys) {
            this.ddlURL = this.ddlFkeysURL;
        } else {
            this.ddlURL = this.base_class.getResource(this.getDDLName(false));
        }
        
    }
    
    public void addTransactionFrequency(Class<? extends VoltProcedure> procClass, int frequency) {
        this.txn_frequencies.put(procClass, frequency);
    }
    public String getTransactionFrequencyString() {
        StringBuilder sb = new StringBuilder();
        String add = "";
        for (Entry<Class<? extends VoltProcedure>, Integer> e : txn_frequencies.entrySet()) {
            sb.append(add).append(e.getKey().getSimpleName()).append(":").append(e.getValue());
            add = ",";
        }
        return (sb.toString());
    }
    
    
    public String getDDLName(boolean fkeys) {
        return (this.project_name + "-ddl" + (fkeys ? "-fkeys" : "") + ".sql");
    }
    
    public String getJarName() {
        return (this.project_name + "-jni.jar");
    }
    public File getJarPath() {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        return (new File(testDir + File.separator + this.getJarName()));
    }
    
    public void addPartitions() {
        for (String i[] : this.partitioning) {
            addPartitionInfo(i[0], i[1]);
        } // FOR
    }
    
    @Override
    public void addAllDefaults() {
        addProcedures(this.procedures);
        addSchema(this.ddlURL);
        addPartitions();
    }
    
    /**
     * Get a pointer to a compiled catalog for the benchmark for all the procedures.
     */
    public Catalog createCatalog(boolean fkeys, boolean full_catalog) throws IOException {
        // compile a catalog
        if (full_catalog) {
            this.addProcedures(this.procedures);    
        } else {
            // The TPC-E catalog takes a long time load, so we have the ability
            // to just compile the schema and the first procedure to make things load faster
            this.addProcedures(this.procedures[0]);
        }
        addSchema(fkeys ? this.ddlFkeysURL : this.ddlURL);
        addPartitions();

        String catalogJar = this.getJarPath().getAbsolutePath();
        try {
            boolean status = compile(catalogJar);
            assert (status);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create " + project_name + " catalog [" + catalogJar + "]", ex);
        }

        Catalog catalog = new Catalog();
        try {
            // read in the catalog
            String serializedCatalog = JarReader.readFileFromJarfile(catalogJar, CatalogUtil.CATALOG_FILENAME);
    
            // create the catalog (that will be passed to the ClientInterface
            catalog.execute(serializedCatalog);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load " + project_name + " catalog [" + catalogJar + "]", ex);
        }

        return catalog;
    }
    
    public Catalog getFullCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, true));
    }
    
    public Catalog getSchemaCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, false));
    }
    
    public static AbstractProjectBuilder getProjectBuilder(ProjectType type) {
        AbstractProjectBuilder projectBuilder = null;
        switch (type) {
            case TPCC:
                projectBuilder = new TPCCProjectBuilder();
                break;
            case TPCE:
                projectBuilder = new TPCEProjectBuilder();
                break;
            case TM1:
                projectBuilder = new TM1ProjectBuilder();
                break;
            case AIRLINE:
                projectBuilder = new AirlineProjectBuilder();
                break;
            case AUCTIONMARK:
                projectBuilder = new AuctionMarkProjectBuilder();
                break;
            case MARKOV:
                projectBuilder = new MarkovProjectBuilder();
                break;
            default:
                assert(false) : "Invalid project type - " + type;
        } // SWITCH
        assert(projectBuilder != null);
        return (projectBuilder);
    }
}