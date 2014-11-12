package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

class Lock {
	TransactionId tid;
	Permissions perm;
	
	Lock(TransactionId tid, Permissions perm) {
		this.tid = tid;
		this.perm = perm;
	}
}

class LockEntry {
	HashSet<TransactionId> tids = new HashSet<TransactionId>();
	Permissions perm;
	Queue<Lock> waitingRequests = new LinkedList<Lock>();
	
	LockEntry(TransactionId tid, Permissions perm) {
		tids.add(tid);
		this.perm = perm;
	}
}

class LockManager {
	private HashMap<PageId,LockEntry> locks = new HashMap<PageId,LockEntry>();
	
	
    public synchronized void acquireLock(PageId pid,TransactionId tid,Permissions perm) {
    	if(locks.containsKey(pid) && locks.get(pid).tids.contains(tid)) {
    		return;
    	}
    	if(locks.containsKey(pid)) {
    		if(locks.get(pid).perm.equals(Permissions.READ_ONLY) 
    				/*TODO provisional*/ && locks.get(pid).waitingRequests.size()<1
    				&& locks.get(pid).perm.equals(perm)) {
    			
    			locks.get(pid).tids.add(tid);
    		} 	
    		else {
    			locks.get(pid).waitingRequests.add(new Lock(tid,perm));
//    			while(locks.containsKey(pid)) {
//    				try {
//    					wait();
//    				} catch (InterruptedException e) {
//    					e.printStackTrace();
//    				}
//    			}
    			while (!locks.get(pid).tids.contains(tid)) { /*spin*/ }
    			
    			if (locks.get(pid).perm.equals(Permissions.READ_ONLY)) {
    				if(locks.get(pid).waitingRequests.peek().perm.equals(Permissions.READ_ONLY)) {
    					Lock next = locks.get(pid).waitingRequests.remove();
    					locks.get(pid).tids.add(next.tid);
    				}
    			}
    		}
    	} else {
    		locks.put(pid, new LockEntry(tid,perm));
    	}
    }

    public synchronized void releaseLock(PageId pid,TransactionId tid) {
//        if(locks.get(pid)!=null && locks.get(pid).equals(tid)) {
//        	if(locks.get(pid).tids.size()==1) {
//        		locks.remove(pid);
//        		notifyAll();
//        	} else {
//        		locks.get(pid).tids.remove(tid);
//        	}
//        }
    	if(locks.containsKey(pid) && locks.get(pid).tids.contains(tid)) {
    		locks.get(pid).tids.remove(tid);
    	}
    	if(locks.get(pid).tids.size()<1) {
    		if(locks.get(pid).waitingRequests.size()<1) {
    			locks.remove(pid);
    		}
    		else {
    			Lock l = locks.get(pid).waitingRequests.remove();
    			locks.get(pid).tids.add(l.tid);
    			locks.get(pid).perm = l.perm; 
    		}
    	}
    }

	public boolean checkLock(TransactionId tid, PageId p) {
		if(locks.get(p)==null) {
			return false;
		}
		return locks.get(p).tids.contains(tid);
	}    
}


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private LinkedList<Page> cache;
    private int maxSize;
    private LockManager manager = new LockManager();
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
       maxSize = numPages;
       this.cache = new LinkedList<Page>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
    	/*
    	 * For now (now being Lab 1), I used an LRU removal policy using a queue in which 
    	 * the most recently used page is appended to the rear.
    	 * This is clearly subject to change.
    	 */
    	manager.acquireLock(pid,tid,perm);
    	Page pg = find(pid);
    	if (pg==null) {
        	int tableid = pid.getTableId();        	
        	pg = Database.getCatalog().getDatabaseFile(tableid).readPage(pid);
        	
        	if(cache.size()> maxSize){
        		evictPage();
        	}
        	cache.push(pg);
        }
    	return pg;
    }
    
    private Page find(PageId pid) {
    	for(Page pg:cache) {
    		if (pg.getId().equals(pid)) {
    			cache.remove(pg);
    			cache.push(pg); //Puts most recently accessed page at the top?
    			return pg;		//LRU policy
    		}
    	}
    	return null;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
    	manager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
    	
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return manager.checkLock(tid,p); 
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> arr = file.insertTuple(tid, t);
        
        for (Page pg: arr) {
        	//TODO, this may be problematic
        	pg = this.getPage(tid, pg.getId(), Permissions.READ_WRITE);
        	pg.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	pg.deleteTuple(t);
    	pg.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for(int i=0;i<cache.size();i++) { //TODO, look into possible issues with realignment
    		if(cache.get(i).isDirty()!=null) {
    			flushPage(cache.get(i).getId());
    		}
    	}
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
		file.writePage(Database.getBufferPool().find(pid)); //TODO, may affect LRU policy
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
    	for(Page pg: cache)   { // cosc460
    		if(pg.isDirty()!=null) { //TODO Check efficiency
    			flushPage(pg.getId());
    			manager.releaseLock(pg.getId(), tid);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Page pg = cache.removeLast();
        try {
        	if(pg.isDirty()!=null) {
        		flushPage(pg.getId());
        	}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

}
