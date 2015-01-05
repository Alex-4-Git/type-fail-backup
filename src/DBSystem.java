//package Nov16;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import Lock.LockType;
//import Transaction.TransactionType;

public class DBSystem {

  ArrayList<Site> sites = new ArrayList<Site>(); // all the sites 

  TransactionManager TM = new TransactionManager();  // TM
  CommandManager CM;
  HashMap<String, Integer> snapshot;
  // for RO transaction only, updated after every commit 

  // HashMap<String, Integer> init_snapshot;
  // store all the initial values

  // store all sites that have failed
  ArrayList<Site> failSites = new ArrayList<Site>();

  void Initialize(String path) throws IOException {

    snapshot = new HashMap<String, Integer>();
    //init_snapshot = new HashMap<String, Integer>();
    // start TM
    // start all sites, assign original data to every site
    this.TM = new TransactionManager(); 
    for (int i =1; i<=10; i++) {
      Site s = new Site (i);
      this.sites.add(s);
    }
    for (int i =1; i<=20; i++) {

      this.snapshot.put('x'+Integer.toString(i), 10*i);
      //this.init_snapshot.put('x'+Integer.toString(i), 10*i);

      if (i%2 == 1) {  // odd variables 
        Variable v = new Variable('x'+Integer.toString(i) , 10*i);
        Site s = Site.getSite(this.sites,1+i%10);
        s.addVariable(v);
      }else {  // even variables
        for (int j=1; j<=10; j++ ){
          Variable v = new Variable('x'+Integer.toString(i) , 10*i);
          Site s = Site.getSite(this.sites, j);
          s.addVariable(v);
        }
      }

    }
    //    System.out.println("Please specify your input type. \"F\" for from file, \"T\" for from terminal");
    //    BufferedReader br = 
    //        new BufferedReader(new InputStreamReader(System.in));
    //    String line;
    //    char type; 
    //    String path=""; 
    //    if ((line=br.readLine())!=null){
    //      if (line.startsWith("F") || line.startsWith("f") ) {
    //        type = 'F';
    //        System.out.println("Please provide the path to your file. ");
    //        line=br.readLine();
    //        if (line != null) {
    //          path = line;
    //        }else {
    //          System.out.println("Path cannot be empty. "); 
    //        }
    //      }else {
    //        type = 'T';
    //      }
    //    }else {
    //      System.out.println("Input type cannot be empty. ");
    //      return;
    //    }

    // initialize a command manager
    CM = new CommandManager();
    CM.getCommands('F', path);
  }
  /////////////////////////////////////////////////////////// end of initialization ///////////////////////////////////////////////////////////

  // called after every regular commit, except for those RO transactions
  void updateSnapShot(Transaction t) {  // t wants to commit

    for (Lock lock : t.lockTable) {
      if (lock.type == LockType.exclusive) {  // this transaction writes something
        Variable v = Site.getSite(this.sites,lock.siteId).variables.get(lock.var_name);
        this.snapshot.put(v.name, v.value);
      }
    }

  }


  void handleCommand (String command, int time) {

    Pattern pattern_begin = Pattern.compile("begin\\(([^ ]*)\\)");
    Pattern pattern_beginRO = Pattern.compile("beginRO\\(([^ ]*)\\)");
    Pattern pattern_R = Pattern.compile("R\\(([^ ]*)\\)");
    Pattern pattern_W = Pattern.compile("W\\(([^ ]*)\\)");
    Pattern pattern_dump = Pattern.compile("dump\\(([^ ]*)\\)");
    Pattern pattern_end = Pattern.compile("end\\(([^ ]*)\\)");
    Pattern pattern_fail = Pattern.compile("fail\\(([^ ]*)\\)");
    Pattern pattern_recover = Pattern.compile("recover\\(([^ ]*)\\)");

    Matcher matcher_begin = pattern_begin.matcher(command);
    Matcher matcher_beginRO = pattern_beginRO.matcher(command);
    Matcher matcher_R = pattern_R.matcher(command);
    Matcher matcher_W = pattern_W.matcher(command);
    Matcher matcher_dump = pattern_dump.matcher(command);
    Matcher matcher_end = pattern_end.matcher(command);
    Matcher matcher_fail = pattern_fail.matcher(command);
    Matcher matcher_recover = pattern_recover.matcher(command);

    // create transactions 
    if (matcher_begin.find()) {  // begin regular transaction
      String t_name = matcher_begin.group(1);
      TM.begin(t_name, time, this.snapshot);
    }
    if (matcher_beginRO.find()) {  // begin RO transaction
      String t_name = matcher_beginRO.group(1);
      TM.beginRO(t_name, time, this.snapshot);

    }
    if (matcher_R.find()) {  // read
      String t_v = matcher_R.group(1);
      String[] parts = t_v.split(",");
      String t_name = parts[0];
      String v_name = parts[1];
      //            for (Site site: this.sites) {
      //              System.out.println(site.id);
      //            }

      if (TM.Aborted(t_name)) {  // the transaction has aborted
        System.out.println(t_name+" has aborted. \""+command+"\" cannot be completed. ");
        return;
      }
      Transaction t = this.TM.getTransaction(t_name);
      if ( !Site.canAccess(this.sites, v_name) && t.type == TransactionType.ReadOnly) {
        // if the variable cannot be accessed, and the transaction is Read Only
        // it should wait
        // System.out.println(v_name+" do not exist on any site. ");
        TM.waitList.add(command);
        int num = TM.pendingOperations.get(t_name);
        num++;
        TM.pendingOperations.put(t_name, num);
        return; 
      }
      boolean success = this.read(t, v_name);
      if (success == false) {
        if (TM.abortList.contains(t.name)) {
          return;
        }
        //System.out.println(command+" pending. ");
        TM.waitList.add(command);
        int num = TM.pendingOperations.get(t_name);
        num++;
        TM.pendingOperations.put(t_name, num);
      }
    }
    if (matcher_W.find()) {  // write 
      String t_v_newVal = matcher_W.group(1);
      String[] parts = t_v_newVal.split(",");
      String t_name = parts[0];
      String v_name = parts[1];
      String new_val = parts[2];
      if ( !Site.canAccess(this.sites, v_name)) {
        System.out.println(v_name+" do not exist on any site. ");
        return; 
      }
      if (TM.Aborted(t_name)) {  // the transaction has aborted
        System.out.println(t_name+" has aborted. \""+command+"\" cannot be completed. ");
        return;
      }
      Transaction t = this.TM.getTransaction(t_name);
      boolean success = this.write(t, v_name, Integer.parseInt(new_val));
      if (success == false) {
        if (TM.abortList.contains(t.name)) {
          return;
        }
        //System.out.println(command+" pending. ");
        TM.waitList.add(command);
        int num = TM.pendingOperations.get(t_name);
        num++;
        TM.pendingOperations.put(t_name, num);
      }
    }
    if (matcher_dump.find()) {  //dump
      String content = matcher_dump.group(1);
      if (content.equals("")) {  // dump all sites
        Site.dumpAllSites(this.sites);
        return;
      }
      if (content.startsWith("x")) {  // dump a variable on all sites
        Site.dumpAllVariables(this.sites, content);
      }else {  // dump one site
        Site site = Site.getSite(this.sites, Integer.parseInt(content));
        Site.dump1Site(site);
      }


    }
    if (matcher_end.find()) {  // end of transaction 
      String t_name = matcher_end.group(1);
      if (TM.Aborted(t_name)) {  // the transaction has aborted
        System.out.println(t_name+" has aborted. \""+command+"\" cannot be completed. ");
        return;
      }
      if (TM.Aborted(t_name)) {  // the transaction has aborted
        System.out.println(t_name+" has aborted. ");
        return;
      }
      Transaction t = TM.getTransaction(t_name);
      boolean success = commit(t);
      if (success == false) {
        //System.out.println(command+" pending. ");
        TM.waitList.add(command);
      }
    }
    if (matcher_fail.find()) {  // transaction fails
      String siteId = matcher_fail.group(1);
      this.fail(Integer.parseInt(siteId));
    }
    if (matcher_recover.find()) { // recover a site 
      String siteId = matcher_recover.group(1);
      this.recover(Integer.parseInt(siteId));
    }

  }

  void handleAllCommands() {
    for (int i=0; i< this.CM.getList().size(); i++ ) { // a list of commands at one time stamp 
      int time = i+1;
      ArrayList<String> list = this.CM.getList().get(i);
      for (String command : list) {  // for each command 
        handleCommand(command, time);
        // after handling each command in order of time, we need to handle every pending command again
        ArrayList<String> copy_waitList = new ArrayList<String>();
        for(String pending : TM.waitList) {  // for each pending command
          copy_waitList.add(pending);
        }
        //System.out.println("wait list size: "+copy_waitList.size());
        TM.waitList.clear();
        // reset TM.pendingOperations
        Iterator<String> keySetIterator = TM.pendingOperations.keySet().iterator();
        while(keySetIterator.hasNext()){
          String key = keySetIterator.next();
          TM.pendingOperations.put(key, 0);
        }
        for(String pending : copy_waitList) {  // for each pending command
          handleCommand(pending, time);
        }

      }

    }
    while (TM.waitList.size()!=0) {
      ArrayList<String> copy_waitList = new ArrayList<String>();
      for(String pending : TM.waitList) {  // for each pending command
        copy_waitList.add(pending);
      }
      // System.out.println("wait list size: "+copy_waitList.size());
      TM.waitList.clear();
      // reset TM.pendingOperations
      Iterator<String> keySetIterator = TM.pendingOperations.keySet().iterator();
      while(keySetIterator.hasNext()){
        String key = keySetIterator.next();
        TM.pendingOperations.put(key, 0);
      }
      for(String pending : copy_waitList) {  // for each pending command
        handleCommand(pending, this.CM.getList().size()+1);
      }
    }

  }



  boolean read(Transaction t, String v_name) {
    int siteId = 0;
    int val = 0;
    if (t.type == TransactionType.ReadOnly) {  // read committed value
      // get value from itself's snapshot
      val = t.snapshot.get(v_name);
      System.out.println("Transaction "+t.name+" reads "+v_name+" as "+val+". ");
      return true; 
    }else {  // regular transaction
      if (t.hasSLock(v_name)) {
        siteId = t.getSLock(v_name).siteId;
        val = Site.getSite(this.sites, siteId).variables.get(v_name).value;
        System.out.println("Transaction "+t.name+" reads "+val+" from Site"+siteId+"'s "+v_name+". "); 
        return true;
      }
      if (t.hasEXLock(v_name)) {
        siteId = t.getEXLock(v_name).siteId;
        val = Site.getSite(this.sites, siteId).variables.get(v_name).value;
        System.out.println("Transaction "+t.name+" reads "+val+" from Site"+siteId+"'s "+v_name+". "); 
        return true;
      }
      // try to get S lock on one site,
      // iterate all site to do above 
      // if success, print result
      // if fail, add to waiting list
      for (Site site : this.sites) {
        if (site.variables.containsKey(v_name)) {
          val = site.variables.get(v_name).value;
          if (site.lockTable.containsKey(v_name)) {  // can not get the lock
            if (site.lockTable.get(v_name).get(0).type == LockType.exclusive) {
              Transaction holder = site.lockTable.get(v_name).get(0).holder;
              if (holder.StartTime < t.StartTime) {  // younger waits for lock from older, should abort
                this.abort(t);
                System.out.println("Because it has to wait for an older transaction. ");
                return false;
              }
              continue;  // go to following site
            }else {   // get shared lock
              // acquire lock
              // add lock to site's lock table
              // add lock to transaction's lock table
              siteId = site.id;
              Lock lock = new Lock(LockType.shared, t, siteId, v_name);
              ArrayList<Lock> lockList = site.lockTable.get(v_name);
              lockList.add(lock);
              site.lockTable.put(v_name, lockList);
              t.lockTable.add(lock);
              System.out.println("Transaction "+t.name+" reads "+val+" from Site"+siteId+"'s "+v_name+". ");
              return true;
            }
          }else {
            // acquire lock
            // add lock to site's lock table
            // add lock to transaction's lock table
            siteId = site.id;
            Lock lock = new Lock(LockType.shared, t, siteId, v_name);
            ArrayList<Lock> lockList = new ArrayList<Lock>();
            lockList.add(lock);
            site.lockTable.put(v_name, lockList);
            t.lockTable.add(lock);
            System.out.println("Transaction "+t.name+" reads "+val+" from Site"+siteId+"'s "+v_name+". ");
            return true;
          }
        }else {
          continue; // go to next site
        }
      }
      // if reach here, it means cannot access any site
      return false;
    }

  }
  //////////////////////////////////// end of read function ////////////////////////////////////////////////
  boolean write(Transaction t, String v_name, int newVal) {
    if (t.hasEXLock(v_name)) {
      System.out.println("Transaction "+t.name+" writes "+v_name+" to all possible sites with new value as "+newVal+". ");
      return true;
    }
    // try to get ex locks from all sites, 
    // iterate all sites to do above 
    // if success, write and print result, return true
    // if fail (have to wait for locks), do not get any lock, add to waiting list, return false
    int var_appearence = 0;
    int v_writable_appearence = 0;
    for (Site site : this.sites) {
      if (site.variables.containsKey(v_name)) {
        var_appearence++;
        if ( site.v_writable(t.name, v_name)) {
          v_writable_appearence++;
        }

      }else {

      }

    }
    if (var_appearence == v_writable_appearence) {  // can get all the ex locks
      // acquire ex lock
      // add lock to lock table of the site
      // add lock to lock table the transaction
      // write to each site;
      for (Site site : this.sites) {
        if (site.variables.containsKey(v_name)) {
          Lock lock = new Lock(LockType.exclusive, t, site.id, v_name);
          site.addEXLock(v_name, lock);
          t.lockTable.add(lock);
          Variable var = site.variables.get(v_name);
          var.setVal(newVal);
          site.variables.put(v_name, var);
        }
      }
      System.out.println("Transaction "+t.name+" writes "+v_name+" to all possible sites with new value as "+newVal+". ");
      return true;
    }else {
      for (Site site : this.sites) {
        // if any lock it is wait for is older than the transaction
        // it should abort
        if (site.shouldAbort(t, v_name) == true) {
          this.abort(t);
          System.out.println("Because it has to wait for an older transaction. ");
          break;
        }
      }
      return false; 
    }

  }

  ////////////////////////////////////end of write function ////////////////////////////////////////////////

  // we want to commit (end) a transaction 
  boolean commit(Transaction t) {
    // release all locks of this transaction 
    // release all locks of related sites
    // update snapshot via the transaction's lock table 
    if (TM.pendingOperations.containsKey(t.name)) {
      if (TM.pendingOperations.get(t.name)>0 ) {  // the transaction is pending
        return false;
      }
    }

    updateSnapShot(t); 
    for (Lock lock : t.lockTable) {
      int siteId = lock.siteId;
      Site site = Site.getSite(this.sites,siteId);
      site.deleteLock(lock.type, lock.holder.name, lock.var_name);
    }
    t.lockTable.clear();
    System.out.println("Transaction "+t.name+" commits successfully. ");
    return true;
  }

  //////////////////////////////// abort a transaction ////////////////////////////////////////
  void abort (Transaction t) {
    // release all locks of this transaction 
    // release all locks of related sites
    // roll back all writes via its snapshot

    if (TM.Aborted(t.name)) { // if the transaction has already failed
      return;
    }

    for (Lock lock : t.lockTable) {
      int siteId = lock.siteId;
      Site site = Site.getSite(this.sites, siteId);

      // roll back the variable the lock is on 
      Variable var = site.variables.get(lock.var_name);
      var.value = this.snapshot.get(lock.var_name);
      site.variables.put(lock.var_name, var);

      site.deleteLock(lock.type, lock.holder.name, lock.var_name);
      if (lock.type == LockType.exclusive) {
        Variable v = site.variables.get(lock.var_name);
        v.value = t.snapshot.get(lock.var_name);
        site.variables.put(lock.var_name, v);
      }
    }
    t.lockTable.clear();
    TM.abortList.add(t.name);  // add to TM's abort list
    System.out.print("We abort "+t.name+". ");
  }

  void fail(int siteId) { // a site fails
    // abort all transactions which have locks on this site
    System.out.println("Site"+siteId+" has failed. ");
    Site site = Site.getSite(this.sites,siteId);
    Iterator<String> keySetIterator = site.lockTable.keySet().iterator();
    while(keySetIterator.hasNext()){
      String var = keySetIterator.next();
      ArrayList<Lock> lockList = site.lockTable.get(var);
      for (int i = lockList.size()-1; i>=0; i--) {
        Lock lock = lockList.get(i);
        this.abort(lock.holder);
        System.out.println("Because its site failed. ");
      }
    }
    // add to fail sites list
    this.failSites.add(site);
    // remove from sites list
    this.sites.remove(site);
  }

  void recover(int siteId) {
    // Site.dumpAllVariables(this.sites,"x8");
    // remove from fail site list
    // update values via initial values
    // add to sites list
    Site site = Site.getSite(this.failSites, siteId);
    this.failSites.remove(site);
    Iterator<String> keySetIterator = site.variables.keySet().iterator();
    while(keySetIterator.hasNext()){
      String v_name = keySetIterator.next();
      Variable var = site.variables.get(v_name);
      var.value = this.snapshot.get(v_name);
      site.variables.put(v_name, var);
    }

    /////////////////////////////////////////////modifications////////////////////////////////////////////////////////
    // check every ex lock, 
    // if there is one ex lock on a variable which the site has a copy, 
    // add ex lock to it on the site 
    Iterator<String> it2 = site.variables.keySet().iterator();
    while(it2.hasNext()){  // go through all variables of the recovering site
      String v_name = it2.next();
      for (Site otherSite: this.sites){ // go through all other sites
        if (otherSite.lockTable.containsKey(v_name)) {
          if (otherSite.lockTable.get(v_name).get(0).type == LockType.exclusive) {
            // add ex lock 
            Transaction holder = otherSite.lockTable.get(v_name).get(0).holder;
            // System.out.println("add lock after recovery. "+v_name+" "+holder.name);
            Lock lock = new Lock(LockType.exclusive, holder, siteId, v_name);
            ArrayList<Lock> lockList = new ArrayList<Lock>();
            lockList.add(lock);
            site.lockTable.put(v_name, lockList);
            holder.lockTable.add(lock);
            int new_val = Site.getSite(this.sites, holder.getLock(v_name).siteId).variables.get(v_name).value; 
            // should write new_val to this site
            Variable variable = site.variables.get(v_name);
            variable.value = new_val;
            site.variables.put(v_name, variable);
            break;
          }
        }

      }
    }
    /////////////////////////////////////////////modifications////////////////////////////////////////////////////////
    this.sites.add(siteId-1,site);
    System.out.println("Site"+siteId+" has recoverd. ");
  }

  static public void main (String[] args) throws IOException {
    DBSystem DB = new DBSystem();
    DB.Initialize(args[0]);
    DB.handleAllCommands();

    //    for (Site s : DB.sites) {
    //      Iterator<String> keySetIterator = s.variables.keySet().iterator();
    //      System.out.println(s.id);
    //      while(keySetIterator.hasNext()){
    //        String v_name = keySetIterator.next();
    //
    //        System.out.println(v_name+" "+ s.variables.get(v_name).value);
    //      } 
    //      System.out.println("///////////////////////////////////////////////////////////////////////////////////");
    //    }
    //    for (int i =0; i<DB.CM.getList().size(); i++) {
    //      ArrayList<String> list = DB.CM.getList().get(i);
    //      System.out.print(i+" : "); 
    //      for (String s : list) {
    //        System.out.print(s);
    //        System.out.print(" ; ");
    //      }
    //      System.out.println();
    //    }
  }

}
