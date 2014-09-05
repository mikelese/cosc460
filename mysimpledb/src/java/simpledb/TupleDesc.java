package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tdDesc;
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     * @throws Exception 
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdDesc = new TDItem[typeAr.length];
        
//        if(typeAr.length != fieldAr.length) {
//        	throw new Exception("TupleDesc Error: Invalid input! Field and Type array lengths must match!");
//        }
        
    	for (int i=0;i<tdDesc.length;i++) {
    		tdDesc[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tdDesc = new TDItem[typeAr.length];
    	for (int i=0;i<tdDesc.length;i++) {
    		tdDesc[i] = new TDItem(typeAr[i], null);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdDesc.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if(i>tdDesc.length-1 || i<0) 
    		throw new NoSuchElementException("TupleDesc Error: Out of bounds.");
    	else
    		return tdDesc[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i>tdDesc.length-1 || i<0) 
    		throw new NoSuchElementException("TupleDesc Error: Out of bounds.");
    	else
    		return tdDesc[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i=0;i<tdDesc.length;i++) {
        	if(tdDesc[i].fieldName==null)
        		continue;
        	if (tdDesc[i].fieldName.equals(name)) {
        		return i;
        	}
        }
        throw new NoSuchElementException("TupleDesc Error: No field with name '"+name+"' was found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int bytes = 0;
    	for (TDItem tdi : tdDesc) {
    			bytes += tdi.fieldType.getLen();
    	}
    	return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] newFields = new Type[td1.numFields()+td2.numFields()];
        String[] newNames = new String[td1.numFields()+td2.numFields()];
        
        for(int i=0;i<td1.numFields();i++) {
        	newFields[i] = td1.getFieldType(i);
        	newNames[i] = td1.getFieldName(i);
        }
   
        for(int i=0;i<td2.numFields();i++) {
        	newFields[i+td1.numFields()] = td2.getFieldType(i);
        	newNames[i+td1.numFields()] = td2.getFieldName(i);
        }
        
    	return new TupleDesc(newFields,newNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o==null || !o.getClass().equals(this.getClass())) {
        	return false;
        }
        
        TupleDesc td2 = (TupleDesc) o;   
        
        if(this.numFields() != td2.numFields()) {
        	return false;
        }
        
        for (int i=0;i<this.numFields();i++) {
//        	if(!this.getFieldName(i).equals(td2.getFieldName(i))) {
//        		return false;
//        	}
        	if(!this.getFieldType(i).equals(td2.getFieldType(i))) {
        		return false;
        	}
        }
        return true;
    }
    
    //TODO Decide if this should be implemented.
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
    	String ret = "";
    	for (int i=0;i<tdDesc.length;i++) {
    		ret+=tdDesc[i].fieldName;
    		ret+="(" + tdDesc[i].fieldType.toString() + ")";
    		
    		if(i!=tdDesc.length-1) {
    			ret+=", ";
    		}
    	}
    	return ret;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        List<TDItem> tddList = Arrays.asList(this.tdDesc);
        return tddList.iterator();
    }

}
