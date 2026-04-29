package com.bugbuster.api;

import io.restassured.RestAssured;
import io.restassured.response.Response;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;

public class ExecutorAPI {

    public static Response configureExecutor(String baseUri, Map<String, Object> queryParams){
        return given()
                .baseUri(baseUri)
                .contentType("application/json")
                .queryParams(queryParams != null ? queryParams : new HashMap<>())
                .post("/server-app/testing-integration-app/mozart/job-executor/initialize");
    }
}
