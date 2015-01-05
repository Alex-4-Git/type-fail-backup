//package Nov16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommandManager {

  ArrayList<ArrayList<String>> list;

  ArrayList<ArrayList<String>> getList () {
    return this.list;
  }
  CommandManager () {
    this.list = new ArrayList<ArrayList<String>>();
  }

  void getCommands (char type, String path) throws IOException {
    BufferedReader br ; 
    if (type == 'f' || type == 'F' ) {
      br = new BufferedReader( new FileReader(path) );
    }else{
      if (type == 't' || type == 'T' ) {
        br = 
            new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Please input instructions. ");
        System.out.println("End the input by entering a new line followed by \" the end\". "); 
        System.out.println("For example: ");
        System.out.println("begin(T1)");
        System.out.println("R(T1,x4);R(T1,x5)");
        System.out.println("W(T1,x4,10)");
        System.out.println("end(T1)");
        System.out.println("the end");
      }else {
        System.out.println("Invalid input. ");
        return;
      }
    }
    String line;
    while ((line=br.readLine())!=null){
      if (line.length() ==0) {
        continue; 
      }
      ArrayList<String> lineCommands = new ArrayList<>();
      if ( line.toLowerCase().startsWith("the end")) {
        return;
      }
      if (line.contains(";")) {
        String[] commands = line.split(";");
        for (String command : commands) {
          lineCommands.add(command.replaceAll("\\s",""));
        }
      }else {
        lineCommands.add(line.replaceAll("\\s",""));
      }
      list.add(lineCommands);
    }
    br.close();
  }

  public static void main (String[] args) throws IOException {

    ///////////////////////////////
    //    File file =new File("/Users/kaiwenshen/Desktop/test/fromConsole");
    //    FileWriter fw;
    //////////////////////////////////


    //      try {
    //        fw = new FileWriter(file,true);
    //
    //        BufferedWriter bw = new BufferedWriter(fw);
    //        bw.write(input);
    //        bw.write('\n');
    //        bw.flush();
    //      } catch (IOException e) {
    //        // TODO Auto-generated catch block
    //        e.printStackTrace();
    //      }

        String s = "R(T2,x2)";
        //Pattern pattern_begin = Pattern.compile("begin\\(([^ ]*)\\)");
        //Pattern pattern_beginRO = Pattern.compile("beginRO\\(([^ ]*)\\)");
        Pattern pattern_R = Pattern.compile("R\\(([^ ]*)\\)");
        //Matcher matcher_begin = pattern_begin.matcher(s);
        //Matcher matcher_beginRO = pattern_beginRO.matcher(s);
        Matcher matcher_R = pattern_R.matcher(s);
        if (matcher_R.find())
        {
          System.out.println("R matched. ");
          System.out.println(matcher_R.group(1));
        }
//        if (matcher_beginRO.find())
//        {
//          System.out.println("beginRO matched. ");
//          System.out.println(matcher_beginRO.group(1));
//        }

//    CommandManager cm = new CommandManager();
//    cm.getCommands('f', "/Users/kaiwenshen/Desktop/test/adb");
//
//    for (int i =0; i<cm.getList().size(); i++) {
//      ArrayList<String> list = cm.getList().get(i);
//      System.out.print(i+" : "); 
//      for (String s : list) {
//        System.out.print(s);
//        System.out.print(";");
//      }
//      System.out.println();
//    }

    //    String st = " 12   345 abc    def   ";
    //    //st = st.replaceAll("\\s","");
    //    //st = st.trim();
    //    System.out.println(st);
  }

}
