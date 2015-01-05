//package Nov16;

enum LockType {shared, exclusive};

public class Lock {
  
  LockType type; 
  Transaction holder;
  int siteId;
  String var_name;
  
  Lock (LockType type, Transaction holder, int siteId, String var_name) {
    this.type=type;
    this.holder=holder;
    this.siteId=siteId;
    this.var_name=var_name;
  }
  
}
