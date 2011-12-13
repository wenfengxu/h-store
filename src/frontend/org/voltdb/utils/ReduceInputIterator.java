package org.voltdb.utils;

import java.util.Iterator;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class ReduceInputIterator<K> implements Iterator<VoltTableRow> {
    
    final VoltTable table;
    boolean isAdvanced;
    
    K oldKey;
    public ReduceInputIterator(VoltTable table) {
        this.table = table;
        oldKey = null;
        isAdvanced = false;
    }
    
    public boolean hasKey() {
        boolean result = this.table.advanceRow();
        this.isAdvanced = (result ? true:false);
        return result;
    }
    
    @SuppressWarnings("unchecked")
    public K getKey() {
        return (K) this.table.get(0);
    }
    
    public void resetKey() {
        oldKey = null;
    }

    @Override
    public boolean hasNext() {
        if(this.isAdvanced){
            if(oldKey == this.getKey() || oldKey == null){
                this.isAdvanced = false;
                oldKey = this.getKey();
                return true;
            } else return false;
        } else {
            if(this.hasKey() && (oldKey == this.getKey() && oldKey == null)) {
                oldKey = this.getKey();
                return true;
            }
            else return false;
        }
    }

    @Override
    public VoltTableRow next() {
        VoltTableRow nt = null;
        if (this.isAdvanced) 
             nt = this.table.getRow();
        else if (this.hasNext())
             nt = this.table.getRow();
        return nt;
    }

    @Override
    public void remove() {
        //throw new NotImplementedException("Cannot remove from a VoltTable");
        throw new UnsupportedOperationException("Cannot remove from a VoltTable");
    }
}
