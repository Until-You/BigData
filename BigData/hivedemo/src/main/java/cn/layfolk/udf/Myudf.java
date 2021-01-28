package cn.layfolk.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Myudf extends UDF {

    public String evaluate(String s) {
        if (s == null) {
            return null;
        }
        return s.toUpperCase();
    }
}