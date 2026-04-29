package com.bugbuster.utils;

import com.bugbuster.api.JobAPI;
import io.restassured.response.Response;
import sun.jvm.hotspot.debugger.windows.x86.WindowsX86CFrame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobUtils {

    public static void validateSequentialJobs(String baseUri, List<String> jobIds) throws Exception {
        Long previousEndTime = null;

        for (String jobId : jobIds) {
            Response res = JobAPI.getJobInfo(baseUri, jobId);
            System.out.println("Validating job: " + jobId);
            System.out.println("Response: " + res.asString());
            if (res.statusCode() != 200) {
                throw new RuntimeException("Failed to fetch job info for " + jobId +
                        ". Status: " + res.statusCode());
            }
            Long startTime = res.jsonPath().getLong("startTime");
            Long stopTime = res.jsonPath().getLong("stopTime");
// Validate sequential execution
            if (previousEndTime != null && startTime < previousEndTime) {
                throw new Exception("Blocking queue violation: Job " + jobId +
                        " started at " + startTime +
                        " before previous job ended at " + previousEndTime);
            }
            previousEndTime = stopTime;
        }
        System.out.println("All Jobs executed sequentially (no overlap detected)");
    }

    public static void validateExecutionOrder(String baseUri, List<String> jobIds) throws Exception {
        Long previousStartTime = null;

        for(String jobId:jobIds){
            Response res = JobAPI.getJobInfo(baseUri, jobId);

            System.out.println("Validating order for job: "+ jobId);
            System.out.println("Response: "+ res.asString());

            if(res.statusCode()!=200){
                throw new RuntimeException("Failed to fetch job info for" + jobId);
            }

            long startTime = res.jsonPath().getLong("startTime");
            if(previousStartTime != null && startTime < previousStartTime){
                throw new Exception("Order violation: Job "+ jobId +
                        " started at " + startTime +
                        " before previous job started at "+ previousStartTime);
            }
            previousStartTime =startTime;
        }
        System.out.println("Jobs executed in submission order");
    }

    public static void validateRoundRobin(String baseUri, List<List<String>> jobIdsByBucket,
                                          List<String> bucketNames) throws Exception {
        // Build a map of jobId -> bucketName
        Map<String, String> jobBucketMap = new HashMap<>();
        for (int i = 0; i < jobIdsByBucket.size(); i++) {
            for (String jobId : jobIdsByBucket.get(i)) {
                jobBucketMap.put(jobId, bucketNames.get(i));
            }
        }

        // Fetch startTime for every job and sort by startTime
        List<String> allJobIds = jobIdsByBucket.stream()
                .flatMap(List::stream)
                .collect(java.util.stream.Collectors.toList());

        allJobIds.sort((a, b) -> {
            Response resA = JobAPI.getJobInfo(baseUri, a);
            Response resB = JobAPI.getJobInfo(baseUri, b);
            long startA = resA.jsonPath().getLong("startTime");
            long startB = resB.jsonPath().getLong("startTime");
            return Long.compare(startA, startB);
        });

        // Walk sorted jobs and validate bucket alternates in round robin
        int bucketCount = bucketNames.size();
        for (int i = 0; i < allJobIds.size(); i++) {
            String jobId = allJobIds.get(i);
            String actualBucket = jobBucketMap.get(jobId);
            String expectedBucket = bucketNames.get(i % bucketCount);
            if (!expectedBucket.equals(actualBucket)) {
                throw new Exception("Round robin violation at position " + i +
                        ": expected bucket '" + expectedBucket +
                        "' but got '" + actualBucket + "' for job " + jobId);
            }
        }
        System.out.println("Round robin validated successfully across buckets: " + bucketNames);
    }
}
