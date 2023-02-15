package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
/**
 * 主要用来定义锁的状态，记录锁对应的事务Id以及锁是EXCLUSIVE_LOCK还是SHARED_LOCK
 * tid 事务的id
 * lockType 对应锁的类型
 */
public class Lock {
    public static final int SHARED_LOCK = 0;
    public static final int EXCLUSIVE_LOCK = 1;

    private final TransactionId tid;
    private int lockType;
    public Lock(TransactionId tid, int lockType){
        this.tid = tid;
        this.lockType = lockType;
    }
    public TransactionId getTid(){
        return this.tid;
    }
    public int getLockType(){
        return this.lockType;
    }
    public void setLockType(int lockType){
        this.lockType = lockType;
    }
}
/**
 * 主要是对锁的储存和管理
 * lockMap 对应Page所涉及的多个锁
 * waitingInfo 对事务所等待的页的映射的存储
 */
class LockManager{
    ConcurrentHashMap<PageId, ArrayList<Lock>>lockMap;
    private Map<TransactionId, PageId> waitingInfo;

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
        this.waitingInfo = new ConcurrentHashMap<>();
    }
    /**
     * 如果该page没有锁就把锁加进去
     * 不然就在它的锁里面找一下有没有符合tid的
     * 判断一下tid能不能获得某个Page类型为lockType的锁
     * @param tid 当前的事务
     * @param pid 所要找的页面
     * @param lockType 所要申请的锁的类型
     */
    public synchronized boolean acquiresLock(TransactionId tid, PageId pid, int lockType){

        if(lockMap.get(pid) == null){
            Lock lock = new Lock(tid, lockType);
            ArrayList<Lock> locks = new ArrayList<>();
            // 在对应的页的锁的集合中添加当前的锁，然后将对应关系添加到锁的映射中
            locks.add(lock);
            lockMap.put(pid, locks);
            return true;
        }
        //寻找符合要求的锁
        ArrayList<Lock> locks = lockMap.get(pid);
        for (Lock lock:locks) {
            //在对应页里寻找对应事务的锁
            if(lock.getTid() == tid){
                //如果锁的权限一样
                if(lock.getLockType() == lockType)
                    return true;
                // lockType = EXCLUSIVE_LOCK, acquiresLock = SHARED_LOCK
                //如果锁是写锁
                if(lock.getLockType() == Lock.EXCLUSIVE_LOCK) {
                    lock.setLockType(Lock.SHARED_LOCK);
                    return true;
                }
                // lockType = SHARED_LOCK, acquiresLock = EXCLUSIVE_LOCK
                // 如果是
                else{
                    // If transaction t is the only transaction holding a shared lock on an object o,
                    // t may upgrade its lock on o to an exclusive lock.
                    if(locks.size() == 1){
                        lock.setLockType(Lock.EXCLUSIVE_LOCK);
                        return true;
                    }
                    return false;
                }
            }
        }
        // 只能有一个独有锁的情况
        if(locks.get(0).getLockType() == Lock.EXCLUSIVE_LOCK){
            return false;
        }
        else {
            // 如果锁是SHARED_LOCK
            if (lockType == Lock.SHARED_LOCK) {
                Lock lock = new Lock(tid, lockType);
                locks.add(lock);
                return true;
            }
            else {
                return false;
            }
        }
    }
    /**
     * 释放Page上所有tid申请的锁
     * @param tid 要释放的事务
     * @param pid 所要找的页面
     */
    public synchronized void releasesLock(TransactionId tid, PageId pid){
        ArrayList<Lock> locks = lockMap.get(pid);
        // 遍历把所有tid的事务的锁释放
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            if(lock.getTid() == tid){
                locks.remove(lock);
                //如果对于该页没有锁了，那么应该在映射中删除来避免冗余
                if(locks.size() == 0)
                    lockMap.remove(pid);
                return;
            }
        }
    }
    /**
     * 判断pid上是否有tid的锁
     * @param tid 要查找的事务的id
     * @param pid 所要判断的Page的id
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        // 先判断该Page有没有锁，再判断锁里面有没有tid的锁
        if(lockMap.get(pid) != null){
            ArrayList<Lock> locks = lockMap.get(pid);
            for (Lock lock:locks){
                if (lock.getTid() == tid)
                    return true;
            }
            return false;
        }
        return false;
    }

    /**
     * 基于图判断死锁是否出现
     * 如果Page的锁里面有其他事务，那么看一下其他事务是否简介需要等待当前的事务，如果有，那么就是图中有环
     */

    public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid) {

        List<Lock> holders = lockMap.get(pid);
        if (holders == null || holders.size() == 0) {
            return false;
        }
        List<PageId> pids = getAllLocksByTid(tid);
        // 看一下tid所需要的所有Page有没有死锁的情况
        for (Lock ls : holders) {
            TransactionId holder = ls.getTid();
            if (!holder.equals(tid)) {
                boolean isWaiting = isWaitingResources(holder, pids, tid);
                if (isWaiting) {
                    return true;
                }
            }
        }
        return false;
    }
    /**
     * 判断一下二者是否有依赖关系，其实就是判断是否有环
     * 主要方法是DFS迭代
     * @param tid 要查找的事务的id
     * @param pids toRemove有关系的所有Page
     * @param toRemove 原来要查的
     */
    private synchronized boolean isWaitingResources(TransactionId tid, List<PageId> pids, TransactionId toRemove) {
        // 事务tid所需要的Page
        PageId waitingPage = waitingInfo.get(tid);
        if (waitingPage == null) {
            return false;
        }
        for (PageId pid : pids) {
            if (pid.equals(waitingPage)) {
                return true;
            }
        }
        // 找到Page所有的锁，看一下有没有和我们当下存在环的，这里我们相当于是判断一个环，需要用到DFS
        List<Lock> holders = lockMap.get(waitingPage);
        if (holders == null || holders.size() == 0) return false;
        for (Lock ls : holders) {
            TransactionId holder = ls.getTid();
            if (!holder.equals(toRemove)) {
                boolean isWaiting = isWaitingResources(holder, pids, toRemove);
                if (isWaiting) return true;
            }
        }
        return false;
    }
    /**
     * 找到该事务所涉及的所有的锁然后存在ArrayList返回
     * @param tid 要查找的事务的id
     */
    private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        // 遍历找到事务tid的所有锁然后存在一个ArrayList里面
        for (Map.Entry<PageId, ArrayList<Lock>> entry : lockMap.entrySet()) {
            for (Lock ls : entry.getValue()) {
                if (ls.getTid().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }
}