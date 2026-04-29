package com.bugbuster.utils;

import com.bugbuster.api.JobAPI;
import io.restassured.response.Response;
import sun.jvm.hotspot.debugger.windows.x86.WindowsX86CFrame;

import java.util.Comparator;
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

        Map<String, Long> startTimes = new HashMap<>();
        for (String jobId : allJobIds) {
            startTimes.put(jobId, JobAPI.getJobInfo(baseUri, jobId).jsonPath().getLong("startTime"));
        }
        allJobIds.sort(Comparator.comparingLong(startTimes::get));

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

    public static void validateMaxParallelism(String baseUri, List<String> jobIds,
                                              int maxParallel) throws Exception {
        // Fetch start/stop times for all jobs once
        Map<String, long[]> jobTimings = new LinkedHashMap<>();
        for (String jobId : jobIds) {
            Response res = JobAPI.getJobInfo(baseUri, jobId);
            if (res.statusCode() != 200) {
                throw new RuntimeException("Failed to fetch job info for " + jobId
                        + ". Status: " + res.statusCode());
            }
            long startTime = res.jsonPath().getLong("startTime");
            long stopTime  = res.jsonPath().getLong("stopTime");
            System.out.println("Job " + jobId + " | start: " + startTime + " | stop: " + stopTime);
            jobTimings.put(jobId, new long[]{startTime, stopTime});
        }

        // For each job, count how many others overlap with it
        for (Map.Entry<String, long[]> entry : jobTimings.entrySet()) {
            String jobId   = entry.getKey();
            long start     = entry.getValue()[0];
            long stop      = entry.getValue()[1];
            int concurrent = 0;

            for (Map.Entry<String, long[]> other : jobTimings.entrySet()) {
                if (other.getKey().equals(jobId)) continue;
                long otherStart = other.getValue()[0];
                long otherStop  = other.getValue()[1];
                // Two jobs overlap if neither ends before the other starts
                if (otherStart < stop && otherStop > start) {
                    concurrent++;
                }
            }
            // concurrent is count of OTHER jobs overlapping, total in-flight = concurrent + 1
            if (concurrent + 1 > maxParallel) {
                throw new Exception("Parallelism violation: Job " + jobId
                        + " ran concurrently with " + concurrent + " other job(s)"
                        + ", exceeding maxParallelJobsPerBucket=" + maxParallel);
            }
        }
        System.out.println("Parallelism validated: no window exceeded " + maxParallel
                + " concurrent job(s)");
    }
}
