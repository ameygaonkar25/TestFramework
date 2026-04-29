package com.bugbuster.utils;

import com.bugbuster.api.JobAPI;
import io.restassured.response.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ApiUtils {
    public static Boolean waitForJobState(String baseUri, String jobId,
                                          String expectedState, int maxRetries, int intervalMs)
            throws InterruptedException {
        for (int i = 0; i < maxRetries; i++) {
            Response response = JobAPI.getJobInfo(baseUri, jobId);
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to fetch job status. HTTP status: "
                        + response.statusCode());
            }
            String jobState = response.jsonPath().getString("jobstate");
            System.out.println("Job " + jobId + " state: " + jobState
                    + " (expecting: " + expectedState + ")");
            if (expectedState.equalsIgnoreCase(jobState)) {
                return true;
            }
            Thread.sleep(intervalMs);
        }
        throw new RuntimeException("Job " + jobId + " did not reach state [" + expectedState
                + "] within the expected time.");
    }

    public static List<String> submitJobs(String baseUri, int count, String jobName, Map<String, Object> optionalParams) {
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Response res = JobAPI.submitJob(baseUri, jobName, optionalParams);
            System.out.println("SubmittingJob raw response: " + res.asString());
            if (res.statusCode() != 200) {
                throw new RuntimeException("Failed to submit job. HTTP status: " + res.statusCode());
            }
            String jobId = res.asString();
            if (jobId == null || jobId.isEmpty()) {
                throw new RuntimeException("jobId is null/Empty for job: " + jobName);
            }
            jobIds.add(jobId.trim());
        }
        return jobIds;
    }

    public static List<String> submitTimeConsumingJobs(String baseUri, int count, String priority) {
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Response res = JobAPI.submitTimeConsumingJob(baseUri, priority);
            System.out.println("SubmittingJob raw response: " + res.asString());
            if (res.statusCode() != 200) {
                throw new RuntimeException("Failed to submit job. HTTP status: " + res.statusCode());
            }
            String jobId = res.asString();
            if (jobId == null || jobId.isEmpty()) {
                throw new RuntimeException("jobId is null/Empty for job: ");
            }
            jobIds.add(jobId.trim());
        }
        return jobIds;
    }

    public static int countJobsByStatus(String baseUri, List<String> jobIds, String status) {
        int count = 0;
        for (String jobId : jobIds) {
            Response res = JobAPI.getJobInfo(baseUri, jobId);
            if (status.equalsIgnoreCase(res.jsonPath().getString("jobState"))) {
                count++;
            }
        }
        return count;
    }

    public static String getJobState(String baseUri, String jobId) {
        Response response = JobAPI.getJobInfo(baseUri, jobId);
        return response.jsonPath().getString("jobState");
    }

    public static Boolean validateJobState(String baseUri, String expectedJobState, String jobId){
        return Objects.equals(getJobState(baseUri,jobId), expectedJobState);
    }
}
