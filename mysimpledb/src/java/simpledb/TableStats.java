package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.Predicate.Op;
import simpledb.TupleDesc.TDItem;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
    
    private Object[] hists;
    private HeapFile file;
    private int numTuples = 0;
    private int[] numDistinct;

    static /*final*/ int IOCOSTPERPAGE;
    
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s;
			s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage){
        this.IOCOSTPERPAGE = ioCostPerPage;
        this.file = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
       
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        this.numDistinct = new int[td.numFields()];
        ArrayList<HashSet<Field>> arr = new ArrayList<HashSet<Field>>();
                
        SeqScan ss = new SeqScan(null,tableid);
        Iterator<TDItem> it;
        try {
            ss.open();
			Tuple temp = ss.next();
			numTuples++;
			
			Tuple min = new Tuple(td);
			Tuple max = new Tuple(td);
			
			for(int i=0;i<td.numFields();i++) {
				min.setField(i, temp.getField(i));
				max.setField(i, temp.getField(i));
			}

			this.hists = new Object[td.numFields()];
			while(ss.hasNext()) {
				temp = ss.next();
				it = td.iterator();
				int index = 0;
				while(it.hasNext()) {
					TDItem tdi = it.next();
					if(tdi.fieldType.equals(Type.INT_TYPE)) {
						//Long story as to why this is the implementation method
						if(((IntField)temp.getField(index)).getValue() > ((IntField)max.getField(index)).getValue()) {
							max.setField(index, temp.getField(index));
						}
						if(((IntField)temp.getField(index)).getValue() < ((IntField)min.getField(index)).getValue()) {
							min.setField(index, temp.getField(index));
						}
					}    
					index++;
				}
				numTuples++;
			}			
						
			ss.rewind();
			for(int i=0;i<hists.length;i++) {
				if(td.getFieldType(i).equals(Type.INT_TYPE)) {
			    	hists[i] = new IntHistogram(NUM_HIST_BINS, ((IntField)min.getField(i)).getValue(), ((IntField)max.getField(i)).getValue());
				} else {
					hists[i] = new StringHistogram(NUM_HIST_BINS);
				}
		    	arr.add(new HashSet<Field>());
			}
			
			while(ss.hasNext()) {
				Tuple tp = ss.next();
				for(int i=0;i<tp.getTupleDesc().numFields();i++) {
					if(tp.getField(i).getType().equals(Type.INT_TYPE)) {
			    		((IntHistogram)hists[i]).addValue(((IntField)tp.getField(i)).getValue());
					} else {
						((StringHistogram)hists[i]).addValue(((StringField)tp.getField(i)).getValue());
					}
		    		arr.get(i).add(tp.getField(i));
				}
			}
			for(int i=0;i<numDistinct.length;i++) {
				numDistinct[i] = arr.get(i).size();
			}
		} catch (NoSuchElementException | TransactionAbortedException
				| DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }
    

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.file.numPages()*IOCOSTPERPAGE;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	System.out.println(this.numTuples);
        double temp = selectivityFactor * (double)this.numTuples;
        if(temp > 0 && temp < 1) {
        	return 1;
        } else {
        	return (int)temp;
        }
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
    	return numDistinct[field];
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if(file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)) {
    		return ((IntHistogram)hists[field]).estimateSelectivity(op, ((IntField)constant).getValue());
    	} else {
    		return ((StringHistogram)hists[field]).estimateSelectivity(op, ((StringField)constant).getValue());
    	}
    }

}
