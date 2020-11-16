package flinkUDF;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

public class StringToArray extends ScalarFunction {


    public String[] eval(String data, String delimiter) {

        try {
            String[] datas = data.split(delimiter);
            List<String> list = new ArrayList<String>();
            for (String tmp : datas) {
                if (!tmp.isEmpty()) {
                    list.add(tmp);
                } else {
                    continue;
                }
            }

            String[] returnArray = list.toArray(new String[0]);
            return returnArray;
        } catch (Exception e) {
            return new String[]{};
        }
    }


    public static void main(String[] args) {
        StringToArray stringToArray = new StringToArray();
        String[] strings = stringToArray.eval("V000005,V000001", ",");

        for (String tmp : strings) {
            System.out.println(tmp);
        }

    }
}