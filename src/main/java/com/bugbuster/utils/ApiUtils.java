package com.bugbuster.utils;

import com.bugbuster.api.JobAPI;
import io.restassured.response.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ApiUtils {

    public static Boolean waitForJobCompletion(String jobId, int maxRetries, int intervalMs) throws InterruptedException {
        for (int i = 0; i < maxRetries; i++) {
            Response response = JobAPI.getJobInfo(jobId);

            System.out.println("Raw response for job " + jobId + ":");
            System.out.println(response.asString());
            // Ensure server returned 200 before parsing JSON
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to fetch job status. HTTP status: " + response.statusCode());
            }

            String jobState = response.jsonPath().getString("jobstate");
            System.out.println("Job " + jobId + " state: " + jobState);
            if ("SUCCESSFUL".equalsIgnoreCase(jobState)) {
                System.out.println("Job " + jobId + " completed successfully.");
                return true;
            }
            Thread.sleep(intervalMs);
        }
        throw new RuntimeException("Job "+ jobId + " did not complete successfully within the expected time.");
    }

    public static List<String> submitJobs(String baseUri, int count, String jobName, Map<String,Object> optionalParams) {
        List<String> jobIds = new ArrayList<>();
        for(int i = 0;i< count; i++){
            Response res = JobAPI.submitJob(baseUri,jobName, optionalParams);
            System.out.println("SubmittingJob raw response: "+ res.asString());
            if(res.statusCode() != 200){
                throw new RuntimeException("Failed to submit job. HTTP status: "+ res.statusCode());
            }
            String jobId = res.asString();
            if(jobId ==null || jobId.isEmpty()){
                throw new RuntimeException("jobId is null/Empty for job: "+ jobName);
            }
            jobIds.add(jobId.trim());
        }
        return jobIds;
    }
}
