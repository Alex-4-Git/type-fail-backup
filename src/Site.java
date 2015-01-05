//package Nov16;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

//import Lock.LockType;

public class Site {

  int id;
  Site (int id) {
    this.id = id; 
    variables = new HashMap<String,Variable>();
    lockTable = new HashMap<String,ArrayList<Lock>>();
  }

  HashMap<String, Variable> variables = new HashMap<String, Variable>();  // maps from variable name to a variable obj
  HashMap<String, ArrayList<Lock>> lockTable = new HashMap<String, ArrayList<Lock>>(); // maps from variable name to its lock 
  // about lockTable: if a variable maps to shared locks, the lock list is of size more than 1, 
  // if a variable maps to a exclusive lock, the lock list is of size 1.

  static Site getSite (ArrayList<Site> list, int id) {
    for (Site site : list) {
      if (site.id == id) {
        return site; 
      }
    }
    return null;
  }

  // check if a variable exist on any of the working sites
  // if a variable don't exist, return false
  static boolean canAccess(ArrayList<Site> list, String v_name) { 
    for (Site site : list) {
      if (site.variables.containsKey(v_name)) {
        return true;
      }
    }
    return false;
  }

  boolean v_writable (String t_name, String v_name) {  // check if writable
    // 
    if (this.lockTable.containsKey(v_name)) {
      for ( Lock lock : this.lockTable.get(v_name)) {
        if ( ! lock.holder.name.equals(t_name)) {
          return false; 
        }

      }
      return true;
    }
    else {
      return true;
    }
  }

  boolean shouldAbort(Transaction t, String v_name) {
    // if the transaction is expecting a lock from an older transaction
    // it should abort
    if (this.lockTable.containsKey(v_name)) {
      for (Lock lock : this.lockTable.get(v_name)) {
        Transaction holder = lock.holder;
        if (holder.StartTime < t.StartTime) {  // younger waits for lock from older, should abort
          return true;
        }
      }
      return false;
    }else {
      return false;
    }

  }

  void addEXLock (String v_name, Lock lock) {  // 
    ArrayList<Lock> lockList = new ArrayList<Lock>();
    lockList.add(lock);
    this.lockTable.put(v_name, lockList);

  }

  void deleteLock (LockType type, String holder_name, String var_name) {
    if (this.lockTable.containsKey(var_name)) {
      for (int i=this.lockTable.get(var_name).size()-1; i>=0; i--) {
        Lock lock = this.lockTable.get(var_name).get(i);
        if (lock.type == type && lock.holder.name.equals(holder_name) && lock.var_name.equals(var_name)) {
          this.lockTable.get(var_name).remove(lock);
        }
      }
      if (this.lockTable.get(var_name).size() ==0) {
        this.lockTable.remove(var_name);
      }
    }else {
      return;
    }
  }

  void addVariable (Variable v) {  // for initialization only
    this.variables.put(v.name , v);
  }

  static void dumpAllSites(ArrayList<Site> list) {
    for (Site site : list) {
      Site.dump1Site(site);
    }
  }

  static void dumpAllVariables(ArrayList<Site> list, String v_name) {
    for (Site site : list) {
      ArrayList<Variable> list2 = new ArrayList<Variable>();
      Iterator<String> keySetIterator = site.variables.keySet().iterator();
      while(keySetIterator.hasNext()){
        String key = keySetIterator.next();
        if (key.equals(v_name)) {
          list2.add(site.variables.get(key));
          continue;
        }
      }
      if (list2.size() != 0) {
        System.out.print("Site"+site.id+": ");
        System.out.print(list2.get(0).name+": "+list2.get(0).value+"; ");
        System.out.println();
      }
    }
  }

  static void dump1Site(Site site) {
    System.out.print("Site"+site.id+": ");
    Iterator<String> keySetIterator = site.variables.keySet().iterator();
    ArrayList<Variable> vars = new ArrayList<Variable>();
    while(keySetIterator.hasNext()){
      String key = keySetIterator.next();
      vars.add(site.variables.get(key));
    }
    Collections.sort(vars,new VariableComparator());
    for (Variable v : vars) {
      System.out.print(v.name+": "+v.value+"; ");
    }
    System.out.println();
  }

  static public void main(String[] args) {
    //    HashMap<String, Integer> snapshot = new HashMap<String, Integer>();
    //    snapshot.put("one", 1);
    //    snapshot.put("two", 2);
    //    System.out.println(snapshot);
    //    System.out.println();
    //    snapshot.clear();
    //    snapshot.put("three", 3);
    //    System.out.println(snapshot);
    //    System.out.println();
  }
}
