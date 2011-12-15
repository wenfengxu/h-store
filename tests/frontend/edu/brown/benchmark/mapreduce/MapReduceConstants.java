/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.mapreduce;

public abstract class MapReduceConstants {
    
    public enum ExecutionType {
        SAME_PARTITION,
        SAME_SITE,
        SAME_HOST,
        REMOTE_HOST,
        RANDOM;
    }
    
    // ----------------------------------------------------------------
    // TABLE INFORMATION
    // ----------------------------------------------------------------
    
	public static final String TABLENAME_TABLEA = "TABLEA";
	public static final long TABLESIZE_TABLEA = 1000000l;
	public static final long BATCHSIZE_TABLEA = 10000l;
	
	public static final int NUM_UNIQUE_NAMES = 10;
	public static final String NAME_PREFIX = "Obama";
	public static final int MAX_AGE = 100;

	public static final String TABLENAME_TABLEB = "TABLEB";
	public static final double TABLESIZE_TABLEB_MULTIPLIER = 10.0d;
	//public static final long TABLESIZE_TABLEB = Math.round(MapReduceConstants.TABLESIZE_TABLEA * TABLESIZE_TABLEB_MULTIPLIER);
	public static final long TABLESIZE_TABLEB = 1000000l;
	public static final long BATCHSIZE_TABLEB = 10000l;
	
	public static final String TABLENAME_WAREHOUSE = "WAREHOUSE";
    public static final long TABLESIZE_WAREHOUSE = 1000000l;
    public static final long BATCHSIZE_WAREHOUSE = 10000l;
    
    public static final String TABLENAME_DISTRICT = "DISTRICT";
    public static final long TABLESIZE_DISTRICT = 1000000l;
    public static final long BATCHSIZE_DISTRICT = 10000l;
	
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final long TABLESIZE_CUSTOMER = 1000000l;
    public static final long BATCHSIZE_CUSTOMER = 10000l;
    
    public static final String TABLENAME_HISTORY = "HISTORY";
    public static final long TABLESIZE_HISTORY = 1000000l;
    public static final long BATCHSIZE_HISTORY = 10000l;
    
    public static final String TABLENAME_STOCK = "STOCK";
    public static final long TABLESIZE_STOCK = 1000000l;
    public static final long BATCHSIZE_STOCK = 10000l;
    
    public static final String TABLENAME_ORDERS = "ORDERS";
    public static final long TABLESIZE_ORDERS = 1000000l;
    public static final long BATCHSIZE_ORDERS = 10000l;

    
    public static final String TABLENAME_NEW_ORDER = "NEW_ORDER";
    public static final long TABLESIZE_NEW_ORDER = 1000000l;
    public static final long BATCHSIZE_NEW_ORDER = 10000l;
    
    public static final String TABLENAME_ORDER_LINE = "ORDER_LINE";
    public static final long TABLESIZE_ORDER_LINE = 1000000l;
    public static final long BATCHSIZE_ORDER_LINE = 10000l;

    public static final String[] TABLENAMES = {
        TABLENAME_TABLEA,
        TABLENAME_TABLEB,
        TABLENAME_WAREHOUSE,
        TABLENAME_DISTRICT,
        TABLENAME_CUSTOMER,
        TABLENAME_HISTORY,
        TABLENAME_STOCK,
        TABLENAME_ORDERS,
        TABLENAME_NEW_ORDER,
        TABLENAME_ORDER_LINE
    };
    
    // ----------------------------------------------------------------
    // STORED PROCEDURE INFORMATION
    // ----------------------------------------------------------------

	public static final int FREQUENCY_MOCK_MAPREDUCE = 30;
	public static final int FREQUENCY_NORMAL_WORDCOUNT = 30; 
	public static final int FREQUENCY_MR_QURERY1 = 20;
	public static final int FREQUENCY_QURERY1 = 20;
    // The number of TABLEB records to return per GetLocal/GetRemote invocation
    public static final int GET_TABLEB_LIMIT        = 10;
}
