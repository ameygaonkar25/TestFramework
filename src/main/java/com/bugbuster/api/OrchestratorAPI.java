package com.bugbuster.api;

import io.restassured.RestAssured;
import io.restassured.response.Response;

import static io.restassured.RestAssured.given;

public class OrchestratorAPI {

    public static Response startOrchestrator(String baseUri) {
        RestAssured.baseURI =baseUri;
        RestAssured.port = 8080;
        return given()
                .contentType("application/json")
                .post("/server-app/testing-integration-app/mozart/orchestrator/initialize");
    }
}
