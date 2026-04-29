package com.bugbuster.api;

import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.restassured.response.Response;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;

public class JobAPI {
    public static Response submitJob(String baseUri, String jobName, Map<String, Object> optionalParams) {
        return given()
                .baseUri(baseUri)
                .contentType("application/json")
                .pathParam("jobName", jobName)
                .queryParams(optionalParams != null ? optionalParams : new HashMap<>())
                .post("/server-app/testing-integration-app/mozart/jobs/submit/{jobName}")
                .extract()
                .response();
    }

    public static Response getJobInfo(String jobId){
        return given()
                .pathParam("jobId", jobId)
                .get("/server-app/testing-integration-app/mozart/jobs/jobInfo/{jobId}")
                .then()
                .extract()
                .response();
    }
}
