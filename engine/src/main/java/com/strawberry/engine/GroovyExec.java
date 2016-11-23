package com.strawberry.engine;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

/**
 * Created by saipkri on 18/11/16.
 */
public class GroovyExec {

    public static void main(String[] args) {
        Binding binding = new Binding();
        binding.setVariable("input", 2);
        GroovyShell shell = new GroovyShell(binding);
        String script = "return 'hello world'+input";
        Object result = shell.evaluate(script);
        System.out.println(result);
    }
}
