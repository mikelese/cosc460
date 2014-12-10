package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {    	
    	readOnlyLog.seek(readOnlyLog.length()); // undoing so move to end of logfile
        long pointer = readOnlyLog.length()-LogFile.LONG_SIZE;
        for(;;) {
            readOnlyLog.seek(pointer);
        	//find start of record, go to start
        	long start = readOnlyLog.readLong();
        	long recordPtr = start;
        	System.out.println("Start: "+ start);
        	readOnlyLog.seek(recordPtr);
        	
        	//read type
        	int type = readOnlyLog.readInt();
        	
        	recordPtr+=LogFile.INT_SIZE;
        	readOnlyLog.seek(recordPtr);
        	
        	//get and compare tid
        	long tid = readOnlyLog.readLong();
        	if(tid == tidToRollback.getId()) {
        		switch(type) {
            	
            	case LogType.ABORT_RECORD:
            		System.out.println("abort");
            		break;
            		
            	case LogType.BEGIN_RECORD:
            		System.out.println("begin");
            		return;
            		
            	case LogType.CHECKPOINT_RECORD:
            		System.out.println("chkpt");
            		break;

            	
            	case LogType.CLR_RECORD:
            		System.out.println("clr");
            		break;

            		
            	case LogType.COMMIT_RECORD:
            		throw new IOException("LogFileRecovery Error: Abort of committed transaction");
            		
            	case LogType.UPDATE_RECORD:
            		System.out.println("update");
            		Page before = LogFile.readPageData(readOnlyLog);
            		Page after = LogFile.readPageData(readOnlyLog);
            		DbFile f = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
            		f.writePage(before);
            		System.out.println("wrote page " + before);
            		Database.getBufferPool().discardPage(before.getId());
            		Database.getLogFile().logCLR(tid, after);
            		
            		break;
            		
            	default:
            		System.out.println("Improper read");

            	}
        	}
        	//set pointer to beginning of terminal file length long 
        	pointer = start - LogFile.LONG_SIZE;
        	//System.out.println("ptr: " + pointer);
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
    	long checkpointLoc = readOnlyLog.readLong();
    	ArrayList<Long> tids = new ArrayList<Long>();

    	if(checkpointLoc != -1) {
    		readOnlyLog.seek(checkpointLoc);
    		readOnlyLog.readInt();//Type
    		readOnlyLog.readLong();//TID
        	int activeTransactions = readOnlyLog.readInt();//Number of transactions
        	for(int i=0;i<activeTransactions;i++) {
        		tids.add(readOnlyLog.readLong());
        	}
        	readOnlyLog.readLong();
    	}	
    	
    	while(readOnlyLog.getFilePointer() < readOnlyLog.length()) {
    		int type = readOnlyLog.readInt();    		
    		long tid = readOnlyLog.readLong();
    		
    		switch(type) {
    		
        	case LogType.ABORT_RECORD:
        		System.out.println("RECOVER abort");
        		tids.remove(tid);
        		break;
        		
        	case LogType.BEGIN_RECORD:
        		System.out.println("begin");
        		tids.add(tid);
        		break;
        		
        	case LogType.CHECKPOINT_RECORD:
        		System.out.println("checkpoint");
        		return;

        	case LogType.CLR_RECORD:
        		System.out.println("clr");
        		LogFile.readPageData(readOnlyLog); // Skip entry
        		break;

        		
        	case LogType.COMMIT_RECORD:
        		System.out.println("commit");
        		tids.remove(tid);
        		break;
        		
        	case LogType.UPDATE_RECORD:
        		System.out.println("update");
        		Page before = LogFile.readPageData(readOnlyLog);
        		Page after = LogFile.readPageData(readOnlyLog);
        		DbFile f = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
        		f.writePage(after);
        		//System.out.println("wrote page " + after);
        		Database.getBufferPool().discardPage(before.getId());
        		break;
        	
        	default:
        		System.out.println("Improper read");
    		}
    		readOnlyLog.readLong();//Offset
    	}  	
    	System.out.println("LOSERS: " + tids);
    	
    	//Reinventing the wheel, I know...
    	
    	
    }
}
