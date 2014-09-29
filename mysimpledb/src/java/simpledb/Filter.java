package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate p;
    private DbIterator[] children;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        children = new DbIterator[1];
        this.children[0] = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return children[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        children[0].open();
    }

    public void close() {
        children[0].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        children[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	
        while (children[0].hasNext()) {
        	Tuple t = children[0].next();
        	if(p.filter(t)) {
        		return t;
        	}
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.children = children;
    }
}