package simpledb;

import java.util.ArrayList;

import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private final int min;
	private final int max;
	//TODO make private
	public int[] buckets;
    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        //if(max > min)
    	
    	this.min = min;
        this.max = max;
        
        if(max-min+1 >buckets) { 
        	this.buckets = new int[buckets];
        } else {
        	this.buckets = new int[max-min+1];
        }
    }
    //TODO Make private
    public int bucketsize(int index) {
    	int ret = (max-min+1)/buckets.length;
    	if(index==buckets.length-1 && (max-min+1)%buckets.length != 0) {
    		ret+=(max-min+1)%buckets.length;
    	}
    	return ret;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (v>max || v<min) {
    		throw new RuntimeException("Illegal value provided. Value must be within the specified range.");
    	}
    	//System.out.println("Adding " + v + " to " + getBucket(v));
    	//System.out.println((v-min)/(double)bucketsize(0));
    	buckets[getBucket(v)]++;
    	//System.out.println(getBucket(v));
    }
    
    //TODO Restore to private
    public int getBucket(int v) {
    	//System.out.println("Bucket Size:" + buckets.length);
    	if(v==max) {
    		//put max value into last bucket
    		return buckets.length-1;
    	} 
    	return (int) ((double)(v-min)/(double)bucketsize(0));
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	double ntups = 0;
    	double left = getBucket(v)*bucketsize(getBucket(v))+min;
    	//double right =
    	
    	for (int i:buckets) {
    		ntups+=i;
    	}
    	double numEqual;
    	double numLess;
    	if(v<min) {
    		numLess=0;
    		numEqual=0;
    	}else if (v>max){
    		numLess = ntups;
    		numEqual=0;
    	} else {
    		numEqual = buckets[getBucket(v)]/(double)bucketsize(getBucket(v));
     		numLess=0;
     		     		
     		for(int i=getBucket(v)-1;i>=0;i--) {
        		numLess+=buckets[i];
        	}
     		numLess+= (double)buckets[getBucket(v)]*((v-left)/(double)bucketsize(getBucket(v)));
    	}   	
    			
        switch(op) {
        
        case EQUALS:
        	return numEqual/ntups;
        case NOT_EQUALS:
        	return (ntups-numEqual)/ntups;
        case GREATER_THAN:
        	return (ntups-(numEqual+numLess))/ntups;
        case GREATER_THAN_OR_EQ:
        	return (ntups-numLess)/ntups;
        case LESS_THAN:
        	return numLess/ntups;
        case LESS_THAN_OR_EQ:
        	return (numLess+numEqual)/ntups;
		default:
			//Includes LIKE
			throw new IllegalArgumentException("IntHistogram Error: Unsupported operation type."); 
        }
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String ret = "";
    	int loc = min;
    	
    	for(int i=0;i<buckets.length;i++) {
    		ret +="Bucket "+i+": ";
    		ret += buckets[i] + " (";
    		ret += loc+"-";
    		
    		if(i==buckets.length-1)
    			ret+=max;
    		else
    			ret +=(loc+ bucketsize(i)-1);
    		
    		ret+=")\n";
    		loc = loc+bucketsize(i+1);
    	}
    	
    	return ret;
    }    
}
