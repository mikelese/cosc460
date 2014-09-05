package simpledb;

import java.io.Serializable;
import java.util.Iterator;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private Field[] fields;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.tupleDesc = td;
    	this.fields = new Field[tupleDesc.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if(tupleDesc.getFieldType(i).equals(f.getType()))
        	fields[i] = f;
        else
        	throw new RuntimeException("Tuple Error: Expected field of type '"+tupleDesc.getFieldType(i)+"'");
    }
        

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
        String ret = "";
        for (int i=0;i<fields.length;i++) {
        	ret+=fields[i];
        	if(i!= fields.length-1)
        		ret+="\t";
        }
        return ret;
    }
}