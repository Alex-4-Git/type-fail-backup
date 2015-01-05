//package Nov16;

import java.util.Comparator;

public class VariableComparator implements Comparator<Variable> {
  @Override
  public int compare(Variable v1, Variable v2) {
    int id1 = Integer.parseInt(v1.name.substring(1));
    int id2 = Integer.parseInt(v2.name.substring(1));
    return id1-id2;
  }
}