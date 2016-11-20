package com.strawberry.engine;

import groovy.lang.GroovyShell;

/**
 * Created by saipkri on 18/11/16.
 */
public class GroovyExec {

    public static void main(String[] args) {
        GroovyShell shell = new GroovyShell();
        String script = "return 'hello world'";
        Object result = shell.evaluate(script);
        System.out.println(result);
    }
}
