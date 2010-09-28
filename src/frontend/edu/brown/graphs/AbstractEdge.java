package edu.brown.graphs;

import java.util.*;

import org.json.*;
import org.voltdb.catalog.*;

import edu.uci.ics.jung.graph.util.*;

/**
 * 
 * @author pavlo
 *
 */
public class AbstractEdge extends AbstractGraphElement {
    public enum Members {
        VERTEX0,
        VERTEX1,
        TYPE,
    }
    
    protected final IGraph<AbstractVertex, AbstractEdge> graph;
    
    /**
     * Base constructor
     * @param graph
     * @param vertices
     */
    @SuppressWarnings("unchecked")
    public AbstractEdge(IGraph<? extends AbstractVertex, ? extends AbstractEdge> graph) {
        super();
        this.graph = (IGraph<AbstractVertex, AbstractEdge>)graph;
    }
    
    /**
     * Copy constructor
     * @param graph
     * @param copy
     */
    @SuppressWarnings("unchecked")
    public AbstractEdge(IGraph<? extends AbstractVertex, ? extends AbstractEdge> graph, AbstractEdge copy) {
        super(graph, copy);
        this.graph = (IGraph<AbstractVertex, AbstractEdge>)graph;
        //this.vertices = copy.vertices;
    }
    
    /**
     * 
     * @return
     */
    public IGraph<AbstractVertex, AbstractEdge> getGraph() {
        return this.graph;
    }
    
    public <T> T getAttribute(String key) {
//        System.out.println("EDGE-GET[" + this.graph + "]: " + key);
        return (T)this.getAttribute(this.graph, key);
    }
    
    public Set<String> getAttributes() {
        return this.getAttributes(this.graph);
    }
    
    public void setAttribute(String key, Object value) {
        this.setAttribute(this.graph, key, value);
    }
    
    public boolean hasAttribute(String key) {
        return this.hasAttribute(this.graph, key);
    }
    
    @Override
    public String toString() {
        String ret = "";
        if (this.graph.getEdgeType(this) == EdgeType.DIRECTED) {
            ret = this.graph.getSource(this).toString() + "->" +  this.graph.getDest(this).toString();
        } else {
            String add = "";
            for (AbstractVertex vertex : this.graph.getIncidentVertices(this)) {
                ret += add + vertex.toString();
                add = "--";
            } // FOR
        }
        return (ret);
    }
    
    @Override
    public String debug() {
        String ret = super.debug() + "\n";
        ret += DEBUG_SPACER + " GRAPH: " + this.graph;
        return (ret);
    }
    
    @Override
    protected void toJSONStringImpl(JSONStringer stringer) throws JSONException {
        Members elements[] = new Members[] { Members.VERTEX0, Members.VERTEX1 };
        int idx = 0;
        for (AbstractVertex v : this.graph.getIncidentVertices(this)) {
            assert(v != null);
            stringer.key(elements[idx++].name()).value(v.getElementId());
        } // FOR
        
        // Only store the first character of the edge type (U, D)
        EdgeType edge_type = this.graph.getEdgeType(this);
        stringer.key(Members.TYPE.name()).value(edge_type.name().subSequence(0, 1));
    }
    
    @Override
    protected void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
        Long v0_elementId = object.getLong(Members.VERTEX0.name());
        AbstractVertex v0 = this.graph.getVertex(v0_elementId);
        assert(v0 != null) : "Invalid vertex element id '" + v0_elementId + "' (0)";
        
        Long v1_elementId = object.getLong(Members.VERTEX1.name());
        AbstractVertex v1 = this.graph.getVertex(v1_elementId);
        assert(v1 != null) : "Invalid vertex element id '" + v1_elementId + "' (1)";
        
        // Edge Type
        String edge_type_key = object.getString(Members.TYPE.name());
        EdgeType edge_type = null;
        for (EdgeType e : EdgeType.values()) {
            if (e.name().startsWith(edge_type_key)) {
                edge_type = e;
                break;
            }
        } // FOR
        assert(edge_type != null) : "Invalid edge type key '" + edge_type_key + "'";
        this.graph.addEdge(this, v0, v1, edge_type);
    }
} // END CLASS