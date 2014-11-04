package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator children[];
    private TransactionId tid;
    private boolean wasCalled = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        children = new DbIterator[1];
        children[0] = child;
        this.tid = t;
    }

    public TupleDesc getTupleDesc() {
        return children[0].getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	children[0].open();
    }

    public void close() {
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
       this.close();
       this.open();
       wasCalled = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(wasCalled) {
        	return null;
        }
    	
    	int numChanged = 0;
        
        while(children[0].hasNext()) {
        	try {
				Database.getBufferPool().deleteTuple(tid, children[0].next());
			} catch (NoSuchElementException e)  {
				System.err.println("Delete error");
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Delete error");
				e.printStackTrace();
			} 
        	numChanged++;
        }
    	
        Type type[] = {Type.INT_TYPE};
        Tuple ret = new Tuple(new TupleDesc(type));
        ret.setField(0, new IntField(numChanged));
        wasCalled = true;
        return ret;
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
