package edu.usfca.cs.dfs.filter;


import edu.usfca.cs.dfs.init.Init;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class StarterA {

    /**
     * main func : runs the bloom filter
     * @param args
     */
    public static void main(String[] args) throws IOException {
        System.out.println("::: Bloom Filter :::");

        if(Init.isCorrectArgs(args)) {
            Map<String, String> c = Init.readConfigFileIntoMap(args[0]);
            System.out.println(c);

            int filterSize = Integer.parseInt(c.get("filterSize"));
            int hashes = Integer.parseInt(c.get("hashes"));

            BloomFilter bf = new BloomFilter(filterSize, hashes);

            for (; ; ) {
                Scanner scanner = new Scanner(System.in);
                if (scanner.hasNextLine()) {
                    bf.executeCommand(scanner.nextLine());
                }
            }
        }



    }



}