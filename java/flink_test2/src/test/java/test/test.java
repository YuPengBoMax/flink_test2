package test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class test {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("mda");
        list.add("dfada");
        list.add("fda");
        list.add("wda");
        list.add("bda");
        list.add("sda");

        for (String a : list){
            System.out.println(a);
        }
        list.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.charAt(0)>o2.charAt(0)) {
                    return 1;
                }else {
                    return -1;
                }
            }
        });

        System.out.println();
        for (String a : list){
            System.out.println(a);
        }

    }
}
