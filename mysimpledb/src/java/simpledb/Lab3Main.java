package simpledb;

import java.io.IOException;

public class Lab3Main {
    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        StringField alice = new StringField("alice", Type.STRING_LEN);
        Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
        Filter filterStudents = new Filter(p, scanStudents);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterStudents.open();
        while (filterStudents.hasNext()) {
            Tuple tup = filterStudents.next();
            System.out.println("\t"+tup);
        }
        filterStudents.close();
        Database.getBufferPool().transactionComplete(tid);
        
        
        // SQL Query Select * from Courses, Profs where cid = favorite course
        // join (seqscan of profs and courses)
        TransactionId tid2 = new TransactionId();
        
        SeqScan scanProfs = new SeqScan(tid2, Database.getCatalog().getTableId("profs"));
        SeqScan scanCourses = new SeqScan(tid2, Database.getCatalog().getTableId("courses"));
                
        JoinPredicate jp = new JoinPredicate(scanProfs.getTupleDesc().fieldNameToIndex(scanProfs.getTableName()+".favoriteCourse"),
        		Predicate.Op.EQUALS,scanCourses.getTupleDesc().fieldNameToIndex(scanCourses.getTableName()+".cid"));
        Join joinFav = new Join(jp, scanProfs, scanCourses);
        
        System.out.println("Query results:");
        joinFav.open();
        while (joinFav.hasNext()) {
            Tuple tup = joinFav.next();
            System.out.println("\t"+tup);
        }
        joinFav.close();
        Database.getBufferPool().transactionComplete(tid2);
        
        TransactionId tid3 = new TransactionId();
        
        scanStudents.open();
        SeqScan scanTakes = new SeqScan(tid2, Database.getCatalog().getTableId("takes"));
        System.out.println(scanTakes.getTupleDesc());
                
        JoinPredicate jp2 = new JoinPredicate(scanStudents.getTupleDesc().fieldNameToIndex(scanStudents.getTableName()+".sid"),
        		Predicate.Op.EQUALS,scanTakes.getTupleDesc().fieldNameToIndex(scanTakes.getTableName()+".sid"));
        Join joinTakes = new Join(jp2, scanStudents, scanTakes);
        
        System.out.println("Query results:");
        joinTakes.open();
        while (joinTakes.hasNext()) {
            Tuple tup = joinTakes.next();
            System.out.println("\t"+tup);
        }
        
        joinTakes.close();
        Database.getBufferPool().transactionComplete(tid3);

    }
}