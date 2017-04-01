package com.spark.test.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PlainWordCount {
    private static String path_src;
    private static String path_result;
    private static BufferedReader br = null;
    private static BufferedWriter bw = null;
    private static String line_current = null;
    private static String[] words = null;
    private static List<String> word_list = new ArrayList<String>();

    public static void main(String[] args) {
        path_src = args[0];
        path_result = "./results.txt";
        File file = new File(path_src);
        if (!file.exists()) {
            System.out.println("file " + file + " is not existed, exit");
            return;
        }
        long start1 = System.currentTimeMillis() ;
        System.out.println("####---------- Flatmap ...");
        try {
            //
            //  Flatmap
            //
            br = new BufferedReader(new FileReader(file.getPath()));
            line_current = br.readLine();
            while (line_current != null) {
                words = line_current.split(" |,|\\.");
                for (String s : words) {
                    if (!s.equals(""))
                        word_list.add(s);
                }

                line_current = br.readLine();
            }

            //for (String temp : word_list) {
            //    System.out.println(temp);
            //}

            // HashSet
            //Set<String> hashSet = new HashSet<String>(word_list);
            //for (String str : hashSet) {
            //    System.out.println("word: " + str +
            //            ", occur times: " + Collections.frequency(word_list, str));
            //}
            System.out.println("####---------- Flatmap total time:"+(System.currentTimeMillis()-start1));
            start1 = System.currentTimeMillis() ;
            // 
            // Map and reduce by key
            //
            System.out.println("####---------- Map & Reduce by key ...");
            Map<String, Integer> hashMap = new HashMap<String, Integer>();
            for (String temp : word_list) {
                Integer count = hashMap.get(temp);
                hashMap.put(temp, (count == null) ? 1 : count + 1);
            }
            System.out.println("####---------- Map & Reduce by key total time:"+(System.currentTimeMillis()-start1));
            start1 = System.currentTimeMillis() ;
            //
            // Sort by Value
            //
            System.out.println("####---------- sorting ...");
            List<Map.Entry<String, Integer>> sorted = sortMapByValue(hashMap);
            String key = sorted.get(0).getKey();
            Integer count = sorted.get(0).getValue();
            System.out.println("####---------- most word:"+key+",count:"+count+",total time:"+(System.currentTimeMillis()-start1));


            // TreeMap
            //TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(
            //        hashMap);

            // Record result to another file
            //printMap(treeMap);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("####---------- total time:"+(System.currentTimeMillis()-start1));

        } finally {
            closeInputStream(br);
            closeOutputStream(bw);
        }
    }

    public static List<Map.Entry<String, Integer>> sortMapByValue(Map<String, Integer> oriMap) {  
        //Map<String, String> sortedMap = new LinkedHashMap<String, String>();  
        List<Map.Entry<String, Integer>> entryList = null;
        if (oriMap != null && !oriMap.isEmpty()) {  
            entryList = new ArrayList<Map.Entry<String, Integer>>(oriMap.entrySet());  
            Collections.sort(entryList,  
                    new Comparator<Map.Entry<String, Integer>>() {  
                        public int compare(Map.Entry<String, Integer> entry1,  
                                Map.Entry<String, Integer> entry2) {  
                            int value1 = 0, value2 = 0;  
                            try {  
                                value1 = entry1.getValue();  
                                value2 = entry2.getValue();  
                            } catch (NumberFormatException e) {  
                                value1 = 0;  
                                value2 = 0;  
                            }  
                            return value2 - value1;  
                        }  
                    });  
            //Iterator<Map.Entry<String, String>> iter = entryList.iterator();  
            //Map.Entry<String, String> tmpEntry = null;  
            //while (iter.hasNext()) {  
            //   tmpEntry = iter.next();  
            //    sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());  
            //}  
        }  
        return entryList;  
    }  
    public static void printMap(Map<String, Integer> map) throws IOException {

        bw = new BufferedWriter(new FileWriter(path_result));

        Set<String> keys = map.keySet();
        for (String s : keys) {
            System.out.println("word: " + s +
                    ", times: " + map.get(s));
            writeResult("word: " + s +
                    ", times: " + map.get(s));
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("word: " + entry.getKey() + ", number : "
                    + entry.getValue());
            writeResult("word: " + entry.getKey() + ", number : "
                    + entry.getValue());
        }

    }

    public static void writeResult(String line) throws IOException {

        try {
            if (bw != null) {
                bw.write(line);
                bw.newLine();
                bw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
            closeOutputStream(bw);
        }
    }

    public static void closeOutputStream(BufferedWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeInputStream(BufferedReader reader) {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
