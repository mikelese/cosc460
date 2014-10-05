package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator children[];
    private int tableid;
    private TupleDesc td;
    private boolean wasCalled = false;
    
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        children = new DbIterator[1];
        children[0] = child;
        this.tableid = tableid;
        
        Type type[] = {Type.INT_TYPE};
        this.td = new TupleDesc(type);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	children[0].open();
    	//TODO See if reset applies to open as well as rewind.
    }

    public void close() {
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.close();
    	this.open();   
    	this.wasCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(wasCalled) {
        	return null;
        }
    	
    	int numChanged = 0;
        
        while(children[0].hasNext()) {
        	try {
				Database.getBufferPool().insertTuple(tid, tableid, children[0].next());
			} catch (NoSuchElementException | IOException e) {
				System.err.println("Insert error");
				e.printStackTrace();
			}
        	numChanged++;
        }
    	
        Tuple ret = new Tuple(td);
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
