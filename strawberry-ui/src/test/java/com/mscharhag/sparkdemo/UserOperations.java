package com.mscharhag.sparkdemo;

import com.google.gson.Gson;
import spark.utils.IOUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.mscharhag.sparkdemo.JsonUtil.json;
import static org.junit.Assert.fail;
import static spark.Spark.*;


/**
 * Created by prabvara on 11/24/2016.
 */
public class UserOperations {

    public static void main (String[] args)
    {

        Gson gson = new Gson();
        staticFiles.location("/public");
        get("/", (rq, res) -> "INDEX");

//        UserControllerIntegrationTest userControllerIntegrationTest = new UserControllerIntegrationTest();
  //      userControllerIntegrationTest.request("POST", "/users?name=user3&email=user3@foobar.com");
        UserService userService = new UserService();
        post("/users", (req, res) -> {
            String jsonBody = req.body();
            User user = gson.fromJson(jsonBody, User.class);
            System.out.println(jsonBody);
            userService.createUser(user.getName(), user.getEmail());
            return jsonBody;
        }, json());

        get("/users", (req, res) -> userService.getAllUsers(), json());

        exception(Exception.class, (ex, req, res) -> ex.printStackTrace());


    }
}
