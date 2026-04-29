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
                .then()
                .extract()
                .response();
    }

    public static Response submitTimeConsumingJob(String baseUri, String priority) {
        return given()
                .baseUri(baseUri)
                .queryParam("priority", priority)
                .contentType("application/json")
                .post("/server-app/testing-integration-app/mozart/jobs/submitTimeConsumingJob")
                .then()
                .extract()
                .response();
    }

    public static void setTimeTakenByJobs(String baseUri, int jobTime) {
        given()
                .baseUri(baseUri)
                .pathParam("jobTime", jobTime)
                .contentType("application/json")
                .get("/server-app/testing-integration-app/mozart/jobs/setTimeTakenByJobInMillis?timeTakenInMillis={jobTime}");
    }

    public static Response getJobInfo(String baseUri, String jobId){
        return given()
                .baseUri(baseUri)
                .pathParam("jobId", jobId)
                .get("/server-app/testing-integration-app/mozart/jobs/jobInfo/{jobId}")
                .then()
                .extract()
                .response();
    }
}
