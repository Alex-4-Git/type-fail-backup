//package Nov16;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

//import Lock.LockType;

enum TransactionType{ ReadOnly, Regular }; 

public class Transaction {

  String name;
  TransactionType type;
  // Queue<Operation> queue; 

  int StartTime;  // time when started, to indicate age 

  HashMap<String, Integer> snapshot = new HashMap<String,Integer>();
  // when initialized, copy from DBSystem's snapshot 

  ArrayList<Lock> lockTable = new ArrayList<Lock>(); // keep trace of the self's all locks

  Lock getLock(String v_name) {  // find lock on v_name
    for (Lock lock : this.lockTable) {
      if (lock.var_name.equals(v_name)) {
        return lock;
      }
    }
    return null;
  }
  boolean hasSLock (String v_name) { // check if has a shared lock on variable 
    for (Lock lock : this.lockTable) {
      if (lock.type == LockType.shared && lock.var_name.equals(v_name)) {
        return true; 
      }
    }
    return false;
  }

  Lock getSLock (String v_name) { // get the shared lock on variable 
    for (Lock lock : this.lockTable) {
      if (lock.type == LockType.shared && lock.var_name.equals(v_name)) {
        return lock; 
      }
    }
    return null;
  }

  Lock getEXLock (String v_name) { // get the ex lock on variable 
    for (Lock lock : this.lockTable) {
      if (lock.type == LockType.exclusive && lock.var_name.equals(v_name)) {
        return lock; 
      }
    }
    return null;
  }

  boolean hasEXLock (String v_name) { // check if has a ex lock on variable 
    for (Lock lock : this.lockTable) {
      if (lock.type == LockType.exclusive && lock.var_name.equals(v_name)) {
        return true; 
      }
    }
    return false;
  }

  Transaction(String name, TransactionType t, int startTime) {
    this.type = t;
    this.name = name;
    this.StartTime = startTime;
  }

  int getStartTime() {
    return this.StartTime;
  }

  void abort() {
    // abort the transaction
    // release all locks 

  }

  // history();
  void commit(){
    // release all locks, 

  }
}
