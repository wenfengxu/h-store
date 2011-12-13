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
            //System.out.println("Old Key00 is: " + oldKey);
            //System.out.println("New Key00 is: " + this.getKey());
            if(oldKey == this.getKey() || oldKey == null){
                this.isAdvanced = false;
                oldKey = this.getKey();
                //System.out.println("I am here, Old key: "+ oldKey);
                return true;
            } else return false;
        } else {
            if(this.hasKey()) {
                
                if((oldKey.equals(this.getKey())) || (oldKey == null)) {
                    oldKey = this.getKey();
                    //System.out.println("I am next,Old key: "+ oldKey);
                    return true;
                } else {
                    //System.out.println("I am next01");
                    return false;
                }
            }
            else {
                //System.out.println("I am next02");
                return false;
            }
        }
    }

    @Override
    public VoltTableRow next() {
        //VoltTableRow nt = null;
        assert (this.isAdvanced); 
       
        return  this.table.getRow();
    }

    @Override
    public void remove() {
        //throw new NotImplementedException("Cannot remove from a VoltTable");
        throw new UnsupportedOperationException("Cannot remove from a VoltTable");
    }
}
