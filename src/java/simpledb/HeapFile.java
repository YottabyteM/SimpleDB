package simpledb;

import java.io.*;
import java.util.*;

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

    private TupleDesc td;

    private File f;

    private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs

    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        try{

            RandomAccessFile rAf=new RandomAccessFile(f,"r");
            int offset = pid.getPageNumber()*BufferPool.getPageSize();
            byte[] b=new byte[BufferPool.getPageSize()];
            rAf.seek(offset);
            rAf.read(b, 0, BufferPool.getPageSize());
            HeapPageId hpid=(HeapPageId)pid;
            rAf.close();

            return new HeapPage(hpid, b);
        }catch (IOException e){
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        try {
            PageId pid= page.getId();
            RandomAccessFile rAf=new RandomAccessFile(f,"rw");
            int offset = pid.getPageNumber()*BufferPool.getPageSize();
            byte[] b = new byte[BufferPool.getPageSize()];
            b=page.getPageData();
            rAf.seek(offset);
            rAf.write(b, 0, BufferPool.getPageSize());
            rAf.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> affectedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = null;
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                //page的insertTuple已经负责修改tuple信息来表明其存储在该page上
                page.insertTuple(t);
                page.markDirty(true, tid);
                affectedPages.add(page);
                break;
            }
            else {
                Database.getBufferPool().releasePage(tid,page.getId());
                continue;
            }
        }
        if (affectedPages.size() == 0) {//说明page都已经满了
            //创建一个新的空白的Page
            //HeapPageId npid = new HeapPageId(getId(), numPages());
            HeapPageId npid = new HeapPageId(getId(), numPages());
            HeapPage blankPage = new HeapPage(npid, HeapPage.createEmptyPageData());
            numPage++;
            //将其写入磁盘
            writePage(blankPage);
            //通过BufferPool来访问该新的page
            HeapPage newPage = null;
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, npid, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            affectedPages.add(newPage);
        }
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pagelist = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage affectedPage = null;
        for (int i = 0; i < numPages(); i++) {
            if (i == pid.getPageNumber()) {
                // 利用缓冲池找到并且删除
                affectedPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                affectedPage.deleteTuple(t);
                pagelist.add(affectedPage);
            }
        }
        if (pagelist.size() == 0) {
            throw new DbException("tuple " + t + " is not in this table");
        }
        //返回一个容器
        return pagelist;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }


    public class HeapFileIterator implements DbFileIterator {


        private Iterator<Tuple> i;
        private TransactionId tid;
        private int pgNum;
        private  HeapFile f;


        public HeapFileIterator(TransactionId tid, HeapFile f) {
            this.tid = tid;
            this.f = f;

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgNum = 0;
            i = getTupleLsFrPg(pgNum).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if( i == null){
                return false;
            }
            if(i.hasNext()){
                return true;
            } else if (pgNum < f.numPages()-1){
                if(getTupleLsFrPg(pgNum + 1).size() != 0){
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if(i == null){
                throw new NoSuchElementException("tuple is null");
            }

            if(i.hasNext()){
                Tuple t = i.next();
                return t;
            } else if(!i.hasNext() && pgNum < f.numPages()-1) {
                pgNum ++;
                i = getTupleLsFrPg(pgNum).iterator();
                if (i.hasNext())
                    return i.next();
                else {
                    throw new NoSuchElementException("No more Tuples");
                }

            } else {
                throw new NoSuchElementException("No more Tuples");
            }

        }
        private List<Tuple> getTupleLsFrPg(int pgNum) throws TransactionAbortedException, DbException {

            PageId pageId = new HeapPageId(f.getId(), pgNum);
            Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);

            List<Tuple> tupleList = new ArrayList<Tuple>();

            HeapPage hp = (HeapPage)page;
            Iterator<Tuple> itr = hp.iterator();
            while(itr.hasNext()){
                tupleList.add(itr.next());
            }
            return tupleList;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();

        }

        @Override
        public void close() {
            i = null;

        }

    }
}