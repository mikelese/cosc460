package simpledb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.file = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
	}

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	byte[] data;
    	BufferedInputStream in;
    	try {
			in = new BufferedInputStream(new FileInputStream(file),BufferPool.PAGE_SIZE);
			in.skip(BufferPool.PAGE_SIZE*(pid.pageNumber()-1));
			int bytesRead = 0;
			data = new byte[BufferPool.PAGE_SIZE];
			while (bytesRead < BufferPool.PAGE_SIZE) {
				data[bytesRead]=(byte)in.read();
				bytesRead++;
			}
			in.close();
			
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}        
    	try {
			return new HeapPage((HeapPageId) pid,data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int)(file.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	
    	class HeapFileIterator implements DbFileIterator {
    		HeapFile heapfile;
    		TransactionId tid;
    		HeapPage curPage;
    		Iterator<Tuple> tuples;
    		int pageNum = -1;
    		
    		public HeapFileIterator(HeapFile heapfile,TransactionId tid) {
    			this.heapfile = heapfile;
    			this.tid = tid;
    		}

			@Override
			public void open() throws DbException, TransactionAbortedException {
				if(pageNum == -1) {
					curPage = (HeapPage)Database.getBufferPool().getPage(tid , new HeapPageId(heapfile.getId(),0), null);
					tuples = curPage.iterator();
					pageNum=0;
				}
			}

			@Override
			public boolean hasNext() 
					throws DbException, TransactionAbortedException {
				if(pageNum == -1)
					return false;
				
				if(pageNum < heapfile.numPages()-1)
					return true;
				else if(pageNum==heapfile.numPages()-1 && tuples.hasNext())
					return true;
				else
					return false;
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if(pageNum == -1)
					throw new NoSuchElementException("HeapFileIterator Error: Iterator has not been opened.");
				
				if(hasNext()) {
					if(tuples.hasNext()) {
						return tuples.next();
					} else {
						Database.getBufferPool().discardPage(curPage.getId());
						pageNum++;
						curPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(pageNum,heapfile.getId()), null);
						tuples = curPage.iterator();
						if(tuples.hasNext())
							return tuples.next();
						else //TODO THIS MAY NEED MORE ERROR HANDLING!!! HASNEXT MAY BE FLAWED!
							throw new NoSuchElementException("HeapFileIterator Error: There are no more tuples left to read");
					}
				}
				else
					throw new NoSuchElementException("HeapFileIterator Error: There are no more tuples left to read");
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				if(pageNum != -1) {
					pageNum = 0;
					curPage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(pageNum,heapfile.getId()), null);
					tuples = curPage.iterator();
				} else {
					throw new NoSuchElementException("HeapFileIterator Error: Iterator has not been opened.");
				}
			}

			@Override
			public void close() {
				if(pageNum !=-1)
					Database.getBufferPool().discardPage(curPage.getId());
				pageNum = -1;
			}
    	}
    	return new HeapFileIterator(this,tid);
    }
}

