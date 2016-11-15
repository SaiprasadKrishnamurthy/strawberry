package com.sai.strawberry.api;

import org.HdrHistogram.Histogram;

/**
 * Created by saipkri on 15/11/16.
 */
public class Stats extends Histogram {
    public Stats(int numberOfSignificantValueDigits) {
        super(numberOfSignificantValueDigits);
    }
}
