//package Nov16;

enum Vtype {clean, dirty};
public class Variable  {
  
  String name;
  int value; 
  // int lastModify;  // tick of last modification
  double getVal() {
    return this.value; 
  }
  void setVal(int val) {
    this.value = val;
  }
  Variable(String name , int val) {
    this.value = val;
    this.name = name;
  }

}
