package com.strawberry.engine;

import com.sai.strawberry.api.EventStreamConfig;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.FileReader;

/**
 * Created by saipkri on 17/11/16.
 */
public class ScriptManager {

    public static void main(String[] args) throws Exception {

        EventStreamConfig e = new EventStreamConfig();
        e.setConfigId("ID");
        System.out.println("One");
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        System.out.println("Two");
        Invocable invocable = (Invocable) engine;
        engine.eval(new FileReader("/Users/saipkri/learning/strawberry/engine/src/main/resources/transform.js"));
        System.out.println("Three");
        invocable.invokeFunction("transform", e);
        System.out.println("Four");

    }
}
