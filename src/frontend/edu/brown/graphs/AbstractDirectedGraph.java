package edu.brown.graphs;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.utils.ClassUtil;
import edu.brown.utils.JSONUtil;
import edu.uci.ics.jung.graph.*;
import edu.uci.ics.jung.graph.util.Pair;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.*;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public abstract class AbstractDirectedGraph<V extends AbstractVertex, E extends AbstractEdge> extends DirectedSparseMultigraph<V, E> implements IGraph<V, E> {
    protected static final Logger LOG = Logger.getLogger(AbstractDirectedGraph.class.getName());
    private static final long serialVersionUID = 5267919528011628037L;

    /**
     * Inner state information about the graph
     */
    private final InnerGraphInformation<V, E> inner;

    /**
     * Constructor
     * @param catalog_db
     */
    public AbstractDirectedGraph(Database catalog_db) {
        super();
        this.inner = new InnerGraphInformation<V, E>(this, catalog_db);
    }
    
    public Set<V> getDescendants(V vertex) {
        final Set<V> found = new ListOrderedSet<V>();
        new VertexTreeWalker<V>(this) {
            @Override
            protected void callback(V element) {
                found.add(element);
            }
        }.traverse(vertex);
        return (found);
    }
    
    public List<V> getAncestors(final V vertex) {
        List<V> ancestors = new ArrayList<V>();
        this.getAncestors(vertex, ancestors);
        return (ancestors);
    }
    
    private void getAncestors(final V v, List<V> ancestors) {
        for (V parent : this.getPredecessors(v)) {
            if (!ancestors.contains(parent)) {
                ancestors.add(parent);
                this.getAncestors(parent, ancestors);    
            }
        } // FOR
        return;
    }
    public Set<V> getRoots() {
        Set<V> roots = new HashSet<V>();
        for (V v : this.getVertices()) {
            if (this.getPredecessorCount(v) == 0) {
                roots.add(v);
            }
        } // FOR
        return (roots);
    }

    // ----------------------------------------------------------------------------
    // INNER DELEGATION METHODS
    // ----------------------------------------------------------------------------
    
    public Database getDatabase() {
        return (this.inner.getDatabase());
    }
    
    @Override
    public List<E> getPath(V source, V target) {
        return (this.inner.getPath(source, target));
    }
    @Override
    public List<E> getPath(List<V> path) {
        return (this.inner.getPath(path));
    }
    @Override
    public String getName() {
        return (this.inner.getName());
    }
    @Override
    public void setName(String name) {
        this.inner.setName(name);
    }
    @Override
    public boolean addVertex(V v) {
        this.inner.addVertx(v);
        return super.addVertex(v);
    }
    @Override
    public V getVertex(String catalog_key) {
        return (this.inner.getVertex(catalog_key));
    }
    @Override
    public V getVertex(CatalogType catalog_item) {
        return (this.inner.getVertex(catalog_item));
    }
    @Override
    public V getVertex(Long element_id) {
        return (this.inner.getVertex(element_id));
    }
    @Override
    public void pruneIsolatedVertices() {
        this.inner.pruneIsolatedVertices();
    }

    /**
     * Makes unique copies of the vertices and edges into the cloned graph
     * @param copy_source
     * @throws CloneNotSupportedException
     */
    @SuppressWarnings("unchecked")
    public void clone(IGraph<V, E> copy_source) throws CloneNotSupportedException {
        //
        // Copy Vertices
        //
        for (V v : copy_source.getVertices()) {
            this.addVertex(v);
        } // FOR
        //
        // Copy Edges
        //
        Constructor<E> constructor = null;
        try {
            for (E edge : copy_source.getEdges()) {
                if (constructor == null) {
                    Class<?> params[] = new Class<?>[] { IGraph.class, AbstractEdge.class };  
                    constructor = (Constructor<E>)ClassUtil.getConstructor(edge.getClass(), params);
                }
                Pair<V> endpoints = copy_source.getEndpoints(edge);
                E new_edge = constructor.newInstance(new Object[]{ this, edge });
                this.addEdge(new_edge, endpoints, copy_source.getEdgeType(edge));
            } // FOR
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return;
    }

    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "@" + this.hashCode());
    }
    
    public String debug() {
        return super.toString();
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        GraphUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        GraphUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        GraphUtil.serialize(this, stringer);
    }
    
    @Override
    public void fromJSON(JSONObject jsonObject, Database catalog_db) throws JSONException {
        GraphUtil.deserialize(this, catalog_db, jsonObject);
    }

}