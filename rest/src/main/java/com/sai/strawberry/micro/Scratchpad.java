package com.sai.strawberry.micro;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Properties;

/**
 * Created by saipkri on 17/11/16.
 */
public class Scratchpad {
    public static void main(String[] args) throws Exception {

        /*System.out.println(Strings.commonPrefix("oliviersergent", "oliversargent"));

        System.out.println(StringUtils.getLevenshteinDistance("oliviersergent", "oliversargent"));

        String one = "oliviersergent";
        String two = "oliversargent";

        int longer = one.length() > two.length() ? one.length() : two.length();


        System.out.println(StringUtils.overlay(one, two, 0, longer));


        double[] v1 = {0, 2, 5};
        double[] v2 = {0, 2, 4};
        System.out.println(new EuclideanDistance().compute(v1, v2));

        String vowels = "aeiou";
        int chars = 2;

        recurse(vowels, 0, 2);



    }

    static void recurse(String main, int mainIdx, int n) {

        if (mainIdx >= main.length()) {
            return;
        } else {
            for (int i = mainIdx; i < main.length(); i++) {
                for (int j = i; j < i + n && j < main.length(); j++) {
                    System.out.print(main.toCharArray()[j]);
                }
                System.out.println();
            }
        }
    }*/


        FileInputStream fis = new FileInputStream(new File("/Users/saipkri/learning/strawberry/rest/src/main/resources/data.json"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        System.out.println("30dbd5a43ed73b8f92714b329ed93c5e");
        System.out.println(md5);


        Properties p = new Properties();
        p.load(new FileInputStream("/Users/saipkri/learning/strawberry/rest/src/main/resources/application.properties"));
        System.out.println(p.get("reports.common.CSVEncoder"));

        System.out.println("Default Charset=" + Charset.defaultCharset());

    }

}
