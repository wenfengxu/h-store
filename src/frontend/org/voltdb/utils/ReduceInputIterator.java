package org.voltdb.utils;

import java.util.Iterator;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class ReduceInputIterator<K> implements Iterator<VoltTableRow> {
    
    final VoltTable table;
    boolean isAdvanced;
    boolean isKeySame;
    boolean isFinish;
    
    K oldKey;
    public ReduceInputIterator(VoltTable table) {
        this.table = table;
        oldKey = null;
        isAdvanced = false;
        isKeySame = true;
        isFinish = false;
    }
    
    public boolean hasKey() {
        boolean result = this.table.advanceRow();
        //this.isAdvanced = (result ? true:false);
        return result;
    }
    
    @SuppressWarnings("unchecked")
    public K getKey() {
        return (K) this.table.get(0);
    }
    
    public void resetKey() {
        oldKey = null;
    }

    /*
     * if there is next same key tuple in this VoltTable rows
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if(isFinish == false){
            if(isAdvanced) oldKey = this.getKey();
            
            if(this.hasKey()) {
                if(oldKey == null || oldKey.equals(this.getKey())) { 
                    isAdvanced = true;
                    return true;
                }else {
                    oldKey = null;
                    return false;
                }
            }else {
                isFinish = true;
                return false;
            }
        } else
            return false;
    }

    @Override
    public VoltTableRow next() {
        assert (this.isAdvanced); 
       
        return  this.table.getRow();
    }

    @Override
    public void remove() {
        //throw new NotImplementedException("Cannot remove from a VoltTable");
        throw new UnsupportedOperationException("Cannot remove from a VoltTable");
    }
}
