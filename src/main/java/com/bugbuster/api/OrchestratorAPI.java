package com.bugbuster.api;

import io.restassured.RestAssured;
import io.restassured.response.Response;

import static io.restassured.RestAssured.given;

public class OrchestratorAPI {

    public static Response startOrchestrator(String baseUri) {
        return given()
                .baseUri(baseUri)
                .contentType("application/json")
                .post("/server-app/testing-integration-app/mozart/orchestrator/initialize");
    }
}
