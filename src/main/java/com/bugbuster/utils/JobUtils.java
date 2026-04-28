package com.bugbuster.utils;

import com.bugbuster.api.JobAPI;
import io.restassured.response.Response;

import java.util.List;

public class JobUtils {

    public static void validateSequentialJobs(List<String> jobIds) throws Exception {
        Long previousEndTime = null;

        for (String jobId : jobIds) {
            Response res = JobAPI.getJobInfo(jobId);
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
}
