//package Nov16;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;

//import Transaction.TransactionType;

public class TransactionManager {

  ArrayList<Transaction> transList = new ArrayList<Transaction>(); 

  // list of transactions which aborted
  ArrayList<String> abortList = new ArrayList<String>(); 

  // waiting list
  // store all pending commands
  ArrayList<String> waitList = new ArrayList<String> ();

  HashMap<String,Integer> pendingOperations = new HashMap<String,Integer>(); 
  // maps from a transaction to how many operations it is pending

  Transaction getTransaction (String t_name) {
    for (Transaction t : this.transList) {
      if (t.name.equals(t_name)) {
        return t; 
      }
    }
    return null;
  }

  void begin(String name, int startingTime, HashMap<String, Integer> snapshot) {
    Transaction t = new Transaction(name, TransactionType.Regular, startingTime);

    Iterator<String> keySetIterator = snapshot.keySet().iterator();
    while(keySetIterator.hasNext()){
      String key = keySetIterator.next();
      t.snapshot.put(key, snapshot.get(key));
    }

    this.transList.add(t);
    this.pendingOperations.put(name, 0);
    System.out.println("Regular transaction "+t.name+" begins at time "+t.StartTime+". ");
  }

  void beginRO(String name, int startingTime, HashMap<String, Integer> snapshot) {
    Transaction t = new Transaction(name, TransactionType.ReadOnly, startingTime);

    Iterator<String> keySetIterator = snapshot.keySet().iterator();
    while(keySetIterator.hasNext()){
      String key = keySetIterator.next();
      t.snapshot.put(key, snapshot.get(key));
    }

    this.transList.add(t);
    this.pendingOperations.put(name, 0);
    System.out.println("RO transaction "+t.name+" begins at time "+t.StartTime+". ");
  }

  //  void detectDeadLock(){
  //
  //  }

  void abort(Transaction t) {
    // abort the transaction
    // release all locks 

  }

  boolean Aborted (String t_name) {
    if (abortList.contains(t_name)) {
      return true;
    }else {
      return false;
    }
  }

  void commit(Transaction t) {
    t.commit();
  }

  static public void main (String [] args) {
    ArrayList<String> original = new ArrayList<String>();
    original.add("1");
    original.add("2");
    ArrayList<String> copy_waitList = new ArrayList<String>();
    for(String pending : original) {  // for each pending command
      copy_waitList.add(pending);
    }
    original.clear();
    System.out.println(original);
    System.out.println(copy_waitList);
  }

}
