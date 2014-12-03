package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

class Lock {
	TransactionId tid;
	Permissions perm;
	
	Lock(TransactionId tid, Permissions perm) {
		this.tid = tid;
		this.perm = perm;
	}
	
	public boolean equals(Object o) {
		Lock other;
		if (o instanceof Lock) {
			other = (Lock)o;
		} else {
			return false;
		}
		return this.tid.equals(other.tid) && this.perm.equals(other.perm);		
	}
	
	public String toString() {
		String ret = "";
		ret+="Tid: " + tid;
		ret+=" Perm: " + perm;
		return ret;
	}
 }

class LockEntry {
	HashSet<TransactionId> active = new HashSet<TransactionId>();
	LinkedList<Lock> waitingRequests = new LinkedList<Lock>();
	boolean isReadOnly;
	
	LockEntry(TransactionId tid, Permissions perm) {
		set(tid,perm);
	}
	
	void set(TransactionId tid, Permissions perm) {
		if(active.size()==0) {
			active.add(tid);
			isReadOnly = perm.equals(Permissions.READ_ONLY);
		}
	}
	
	void set(Lock l) {
		set(l.tid,l.perm);
	}
	
	boolean add(TransactionId tid, Permissions perm) {
		if(isReadOnly && perm.equals(Permissions.READ_ONLY)) {
			active.add(tid);
			return true;
		}
		return false;
	}
	
	boolean containsTid(TransactionId tid) {
		return active.contains(tid);
	}
	
	public String toString() {
		String ret = "";
		ret += "Active: ";
		ret += active + "\n";
		ret += "Queue: ";
		ret += waitingRequests;
		return ret;
	}
}

class LockManager {
	private HashMap<PageId,LockEntry> locks = new HashMap<PageId,LockEntry>();
	
	
    public void acquireLock(PageId pid,TransactionId tid,Permissions perm) 
    		throws TransactionAbortedException {
    	//System.out.println(this);
    	synchronized (this) {
    	LockEntry lockentry = locks.get(pid);
    	//No lock has been taken out on this page.
    	if(lockentry==null) {
    		System.out.println(tid+" (New entry)");
    		locks.put(pid,new LockEntry(tid,perm));    		
    		return;
    	}
    	
    	Lock lock = new Lock(tid, perm);
    	    	
    	//Lock is already held
    	if(lockentry.containsTid(tid)) {
    		return;
    	}
    	
    	
    	//LockEntry is initialized, but not in use, add new lock to active set
    	if(lockentry.active.size() == 0) {
    		System.out.println("active is empty, add");
    		lockentry.active.add(tid);
    		lockentry.isReadOnly = perm.equals(Permissions.READ_ONLY);
    		return;
    	}
    	
    	//Read lock request, r/w lock already held 
    	if(lockentry.containsTid(tid) && perm.equals(Permissions.READ_ONLY) && !lockentry.isReadOnly) {
    		return;
    	}
    	
    	//Lock is read only, add read only lock request to active set
//    	if(lockentry.isReadOnly && perm.equals(Permissions.READ_ONLY)) {
//    		lockentry.active.add(tid);
//    		return;
//    	}
    	
    	//Upgrade: Lock is read-only, acquisition request is r/w
    	if(perm.equals(Permissions.READ_WRITE) && lockentry.containsTid(tid)) {
    		if(lockentry.active.size() > 1) {
    			lockentry.waitingRequests.push(lock);
    		} else {
    			lockentry.isReadOnly = false;
    			return;
    		}
    	}
    	    	
    	//Normal case: if lock is not in waiting queue, add
    	if(!lockentry.waitingRequests.contains(lock)) {
    		lockentry.waitingRequests.add(lock);
    		System.out.println(tid + " is added to queue.");
    		System.out.println("active: " + lockentry.active);
    		System.out.println("queue: " + lockentry.waitingRequests);
    	}
    	
    	long start = System.nanoTime();
    	long maximum = 1500 * 1000000;
    	
    	//Spin wait until in the active set
    	while(!lockentry.active.contains(lock)) {
    		//spin
    		if (System.nanoTime()-start > maximum) {
    			throw new TransactionAbortedException();
    		}
    	}
    	}
    }

    public synchronized void releaseLock(PageId pid,TransactionId tid) {
    	LockEntry lockentry = locks.get(pid);
    	HashSet<Lock> remove = new HashSet<Lock>();
		System.out.println("removing " + tid);
		System.out.println("Queue: " + lockentry.waitingRequests);
    	
    	//Remove locks in active set, avoiding concurrent modification
    	if(lockentry.active.contains(tid)) {
    		lockentry.active.remove(tid);
    	}
    	
		System.out.println("Active state: " + lockentry.active);
    			
    	//If there are no other transactions using this lock, add new locks from queue.
    	if(lockentry.active.size()==0) {
    		//If there is an item on queue, add it
    		if(lockentry.waitingRequests.size() >0) {
    			    			
	    		Lock l = lockentry.waitingRequests.pop();					
	    		lockentry.set(l);
	    		
	    		// If this item is read only, and there are items on queue
	    		if(lockentry.isReadOnly && lockentry.waitingRequests.size() >0) {
	    			Lock read = lockentry.waitingRequests.peek();
	    			
	    			//Add items to active set from queue until requests are r/w or there are no requests
	    			//Only pop prior to add
	    			while (read!=null && read.perm.equals(Permissions.READ_ONLY)) {
	    				lockentry.waitingRequests.pop();
	    				lockentry.active.add(read.tid);
	    				read = lockentry.waitingRequests.peek();
	    			}
	    		}
    		}
    	}
    }

	public boolean checkLock(TransactionId tid, PageId p) {
		if(locks.get(p)==null) {
			return false;
		}
		return locks.get(p).containsTid(tid);
	}
	
	//Removes all Locks that have tid
	public synchronized void abort(TransactionId tid) {
		//System.out.println("aborting");
		Iterator<PageId> it = locks.keySet().iterator();
		while(it.hasNext()) {
			PageId pid = it.next();
			LockEntry le = locks.get(pid);
			//System.out.println(le.active);
			HashSet<Lock> removeLocks = new HashSet<Lock>();
			for(Lock l : le.waitingRequests) {
				if(l.tid.equals(tid)) {
					removeLocks.add(l);
				}
			}
			le.waitingRequests.removeAll(removeLocks);
			//System.out.println(le.waitingRequests);
			
			if(le.containsTid(tid)) {
				releaseLock(pid,tid);
			}

		}
	}
	
	public String toString() {
		return locks.toString();
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
    	//System.out.println("get page " + pid);
    	System.out.println("Bufferpool state:" + cache);
    	if(cache.size()>0) {
    		System.out.println(cache.peek().isDirty());
    	}
    	manager.acquireLock(pid,tid,perm);
		//System.out.println("Locked page");
    	Page pg = find(pid);
    	if (pg==null) {
        	int tableid = pid.getTableId();        	
        	pg = Database.getCatalog().getDatabaseFile(tableid).readPage(pid);
        	synchronized (this) {
	        	if(cache.size()>= maxSize){
	        		evictPage();
	        	}
	        	cache.push(pg);
        	}
        }
    	return pg;
    }
    
    private synchronized Page find(PageId pid) {
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
    	transactionComplete(tid,true);
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
    	//Transaction commits successfully, flush pages to disk.
    	if(commit) {
    		flushPages(tid);
    		//System.out.println(manager);
    		manager.abort(tid);
    		
    	//Transaction failed to commit,	remove dirty pages.
    	} else {
    		HashSet<Page> toRemove = new HashSet<Page>();
    		synchronized (this) {
	    		for(Page p: cache) {
	    			//System.out.println(p.isDirty());
	    			if(p.isDirty()==null) {
	    				//System.out.println("NOT DIRTY" + cache.size() +" " + this.maxSize);
	    				continue;
	    			}
	    			
	    			if(p.isDirty().equals(tid)) {
	    				//cache.remove(p);
	    				//System.out.println(cache);
	    				//System.out.println("Page " + p + "(pid: " +p.getId()+ ") is dirty");
	    				//HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(p.getId().getTableId());
	    				//cache.add(file.readPage(p.getId()));
	    				toRemove.add(p);
	    				//releasePage(tid,p.getId());
	    			}
	    		}
	    		for(Page p : toRemove) {
	    			cache.remove(p);
					System.out.println("Page " + p + "(pid: " +p.getId()+ ") is dirty");
					HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(p.getId().getTableId());
					cache.add(file.readPage(p.getId()));
					releasePage(tid,p.getId());
	    		}
    		}
    		manager.abort(tid);
    		//System.out.println(manager);
    	}
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
        System.out.println("ARRAY LIST " + arr);
        for(Page p: arr) {
        	synchronized (this) {
	        	cache.remove(p);
	        	p.markDirty(true, tid);
	        	cache.push(p);
        	}
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
    	cache.remove(pg);
    	pg.markDirty(true, tid);
    	cache.push(pg);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	synchronized (this) {
	    	for(int i=0;i<cache.size();i++) { //TODO, look into possible issues with realignment
	    		if(cache.get(i).isDirty()!=null) {
	    			flushPage(cache.get(i).getId());
	    		}
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
//        for(Page p: cache) {
//        	if (p.getId().equals(pid)) {
//        		cache.remove(p);
//        		return;
//        	}
//        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
		file.writePage(Database.getBufferPool().find(pid));
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
    	HashSet<Page> toRemove = new HashSet<Page>();
    	for(Page pg: cache)   {	
    		if(pg.isDirty() != null && pg.isDirty().equals(tid)) {
    			toRemove.add(pg);
    		}
    	}
    	int i=0;
    	for(Page pg: toRemove) {
    		System.out.println("Releasing page: " + ++i);
        	flushPage(pg.getId());
    		releasePage(tid,pg.getId());
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<Page> descend = cache.descendingIterator(); //LRU implementation
                
        while(descend.hasNext()) {
        	Page pg = descend.next();
        	if(pg.isDirty()==null) {
        		//System.out.println(pg.isDirty());
        	//	System.out.println("found unsullied page " + pg);
        		cache.remove(pg);
        	//	System.out.println(cache);
        		return;
        	} 
        }
        throw new DbException("BufferPool Exception: All BufferPool pages are dirty.");
    }
}
