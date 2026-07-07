package com.bugbuster.mozart;

import com.bugbuster.api.ExecutorAPI;
import com.bugbuster.api.OrchestratorAPI;
import com.bugbuster.utils.ApiUtils;
import com.bugbuster.utils.DBUtils;
import com.bugbuster.utils.JobUtils;
import com.bugbuster.utils.K8sHelper;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MozartApiK8sDynamicTests {

    private String baseUri = "http://server-app.local";

    static DBUtils dbUtils;

    static {
        try {
            dbUtils = new DBUtils();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Test
    @Tag("HA-TC-51")
    @DisplayName("HA-TC-51")
    public void testBlockingQueueSequentialExecution() throws Exception {
        K8sHelper.scaleAndGetPodUrls(1);

        Map<String, Object> config = new HashMap<>();
        String groupName = "BlockingGroup";
        String executorName = "BlockingExecutor";
        config.put("group-name", groupName);
        config.put("executor-name", executorName);
        config.put("thread-pool-size", 10);
        config.put("isBlocking", true);
        config.put("with-persistence", true);

        assertEquals(200, ExecutorAPI.configureExecutor(baseUri, config).getStatusCode());

        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("executor-name", executorName);
        List<String> jobIds = ApiUtils.submitJobs(baseUri,10, "BlankJob", jobParams);
        assertEquals(10,jobIds.size());

        for (String jobId:jobIds){
            assertTrue(ApiUtils.waitForJobState(baseUri, jobId, "SUCCESSFUL", 10, 500));
        }
        JobUtils.validateSequentialJobs(baseUri, jobIds);
    }

    //Bucketing Queue Tests
    @Test
    @Order(1)
    @Tag("BQ-TC-01")
    @DisplayName("BQ-TC-01: Start bucket job executor with withExecuteWithBuckets=true")
    public void testStartBucketExecutor() throws Exception {
        K8sHelper.scaleAndGetPodUrls(1);

        Map<String, Object> config = new HashMap<>();
        String groupName = "BucketingGroup";
        String executorName = "BucketingExecutor";
        config.put("thread-pool-size", 10);
        config.put("group-name", groupName);
        config.put("executor-name", executorName);
        config.put("withExecuteWithBuckets", true);
        config.put("with-persistence", true);

        int statusCode = ExecutorAPI.configureExecutor(baseUri, config).getStatusCode();
        assertEquals(200, statusCode, "Bucket executor failed to start");
        System.out.println("Bucket executor started successfully");
    }

    private static void clearDBTables() throws Exception {
        dbUtils.truncateExecutorTables();
    }

    @AfterAll
    public static void cleanupAfterEachTests() throws Exception {
        K8sHelper.scaleDownPodsToZero();
        clearDBTables();
    }

}