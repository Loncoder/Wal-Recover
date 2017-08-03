package freewheel.rpt;

import org.apache.hadoop.hbase.util.Bytes;

public class Test {
    public static void main(String[] args) {
        String cf = "cf1";
        System.out.println(Bytes.toString(cf.getBytes()));
    }
}
