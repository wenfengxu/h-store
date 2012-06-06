/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.messaging.*;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements FastSerializable {

    String procName = null;
    boolean sysproc = false;
    ParameterSet params = null;
    ByteBuffer unserializedParams = null;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;
    
    /** Whether this invocation should be specifically executed at a particular partition **/
    int base_partition = -1;
    
    /** What partitions this invocation will touch **/
    Set<Integer> partitions = null;
    
    public StoredProcedureInvocation() {
        super();
    }
    
    public StoredProcedureInvocation(long handle, String procName, Object... parameters) {
        super();
        this.clientHandle = handle;
        this.procName = procName;
        this.sysproc = (procName.startsWith("@"));
        this.params = new ParameterSet();
        this.params.setParameters(parameters);
    }
    
    public StoredProcedureInvocation getShallowCopy()
    {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.clientHandle = clientHandle;
        copy.params = params;
        copy.procName = procName;
        if (unserializedParams != null)
        {
            copy.unserializedParams = unserializedParams.duplicate();
        }
        else
        {
            copy.unserializedParams = null;
        }

        return copy;
    }

    public void setProcName(String name) {
        procName = name;
    }

    public void setParams(Object... parameters) {
        // convert the params to the expected types
        params = new ParameterSet();
        params.setParameters(parameters);
        unserializedParams = null;
    }

    public boolean isSysProc() {
        return sysproc;
    }
    
    public String getProcName() {
        return procName;
    }

    public ParameterSet getParams() {
        return params;
    }

    public void setClientHandle(int aHandle) {
        clientHandle = aHandle;
    }
    public long getClientHandle() {
        return clientHandle;
    }
    
    public boolean hasBasePartition() {
        return (this.base_partition != -1);
    }
    public int getBasePartition() {
        return (this.base_partition);
    }
    public void setBasePartition(int partition) {
        this.base_partition = (short)partition;
    }
    
    public boolean hasPartitions() {
        return (this.partitions != null);
    }
    public Set<Integer> getPartitions() {
        return (this.partitions);
    }
    public void addPartitions(Collection<Integer> partitions) {
        if (this.partitions == null) this.partitions = new HashSet<Integer>();
        this.partitions.addAll(partitions);
    }

    
    @Deprecated
    public void buildParameterSet() {
        this.buildParameterSet(new FastDeserializer(unserializedParams));
    }
    
    /**
     * If created from ClientInterface within a single host,
     * will still need to deserialize the parameter set. This
     * optimization is not performed for multi-site transactions
     * (which require concurrent access to the task).
     */
    public void buildParameterSet(FastDeserializer fds) {
       if (unserializedParams != null) {
           assert (params == null);
           try {
               fds.setBuffer(unserializedParams);
               params = fds.readObject(ParameterSet.class);
               unserializedParams = null;
           }
           catch (IOException ex) {
               throw new RuntimeException("Invalid ParameterSet in Stored Procedure Invocation.");
           }
       }
    }

    /** Read into an unserialized parameter buffer to extract a single parameter */
    Object getParameterAtIndex(int partitionIndex) {
        try {
            return ParameterSet.getParameterAtIndex(partitionIndex, unserializedParams);
        }
        catch (IOException ex) {
            throw new RuntimeException("Invalid partitionIndex", ex);
        }
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
//        in.readByte();//skip version
        sysproc = in.readBoolean();
        base_partition = (int)in.readShort();
        clientHandle = in.readLong();
        procName = in.readString();
        
        int num_partitions = in.readShort();
        if (num_partitions > 0) {
            this.partitions = new HashSet<Integer>();
            for (int i = 0; i < num_partitions; i++) {
                this.partitions.add((int)in.readShort());
            } // FOR
        }
        
        // do not deserialize parameters in ClientInterface context
        unserializedParams = in.remainder();
        params = null;
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(!((params == null) && (unserializedParams == null)));
        assert((params != null) || (unserializedParams != null));
//        out.write(0);   // version (1)
        out.writeBoolean(sysproc); // (1)
        out.writeShort(base_partition); // (2)
        out.writeLong(clientHandle);    // (8) 
        out.writeString(procName);
        
        if (this.partitions == null) {
            out.writeShort(0);
        } else {
            out.writeShort(this.partitions.size());
            for (Integer p : this.partitions) {
                out.writeShort(p.intValue());
            } // FOR
        }
        
        if (params != null)
            out.writeObject(params);
        else if (unserializedParams != null)
            out.write(unserializedParams.array(),
                      unserializedParams.position() + unserializedParams.arrayOffset(),
                      unserializedParams.remaining());
    }
    
    /**
     * Returns true if the raw bytes for this invocation indicate that it's a sysproc request
     * @param buffer
     * @return
     */
    public static boolean isSysProc(ByteBuffer buffer) {
        return (buffer.get(0) == 1);
    }
    
    /**
     * Mark a serialized byte array of a StoredProcedureInvocation as being redirected to
     * the given partition id without having to serialize it first.
     * @param partition
     * @param serialized
     */
    public static void markRawBytesAsRedirected(int partition, byte serialized[]) {
        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        buffer.putShort(1, (short)partition);
    }
    
    /**
     * Returns the base partition of this invocation
     * If the base partition is not set, the return value will be -1
     * @param serialized
     * @return
     */
    public static int getBasePartition(ByteBuffer buffer) {
        return (buffer.getShort(1));
    }

    /**
     * 
     * @param buffer
     * @return
     */
    public static String getProcedureName(ByteBuffer buffer) {
        buffer.rewind();
        FastDeserializer in = new FastDeserializer(buffer);
        try {
            in.skipBytes(11);
            return (in.readString());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Return the client handle from the serialized StoredProcedureInvocation without having to 
     * deserialize it first
     * @param serialized
     * @return
     */
    public static long getClientHandle(ByteBuffer buffer) {
        buffer.rewind();
        return (buffer.getLong(3));
    }

    @Override
    public String toString() {
        String retval = "Invocation: " + procName + "(";
        if (params != null)
            for (Object o : params.toArray()) {
                retval += o.toString() + ", ";
            }
        else
            retval += "null";
        retval += ")";
        return retval;
    }

    public void getDumpContents(StringBuilder sb) {
        sb.append("Invocation: ").append(procName).append("(");
        if (params != null)
            for (Object o : params.toArray()) {
                sb.append(o.toString()).append(", ");
            }
        else
            sb.append("null");
        sb.append(")");
    }
}
