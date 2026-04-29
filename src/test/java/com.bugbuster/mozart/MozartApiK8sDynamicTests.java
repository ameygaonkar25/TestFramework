package com.bugbuster.mozart;

import com.bugbuster.api.ExecutorAPI;
import com.bugbuster.api.OrchestratorAPI;
import com.bugbuster.utils.ApiUtils;
import com.bugbuster.utils.JobUtils;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MozartApiK8sDynamicTests {

    private static final String NAMESPACE = "mozart";
    private static final String DEPLOYMENT_NAME = "server-app";
    private static final int PORT = 8080;
    private List<String> activePodUrls;

    private List<String> scaleAndGetPodUrls(int replicas) throws InterruptedException {
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            RollableScalableResource deployment = client.apps()
                    .deployments()
                    .inNamespace(NAMESPACE)
                    .withName(DEPLOYMENT_NAME);
            // Scale deployment
            deployment.scale(replicas);
            System.out.println("Scaled deployment to " + replicas + " replicas.");
            // Wait for pods to be ready (replace with readiness checks if needed)
            waitForPodsReady(client, replicas,120);
            List<Pod> pods = client.pods()
                    .inNamespace(NAMESPACE)
                    .withLabel("app", DEPLOYMENT_NAME)
                    .list()
                    .getItems();
            if (pods.size() < replicas) {
                throw new IllegalStateException("Expected " + replicas + " pods, found " + pods.size());
            }
            return pods.stream()
                    .map(p -> "http://" + p.getStatus().getPodIP() + ":" + PORT)
                    .toList();
        }
    }

    private void waitForPodsReady(KubernetesClient client, int replicas, int timeoutSeconds) throws InterruptedException {
        int waited = 0;
        while (waited < timeoutSeconds) {
            List<Pod> pods = client.pods()
                    .inNamespace(NAMESPACE)
                    .withLabel("app", DEPLOYMENT_NAME)
                    .list().getItems();

            long readyCount = pods.stream().filter(p -> {
                if (p.getStatus() == null || p.getStatus().getConditions() == null) return false;
                return p.getStatus().getConditions().stream()
                        .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
            }).count();

            if (readyCount >= replicas) return;

            Thread.sleep(2000);
            waited += 2;
        }
        throw new IllegalStateException("Pods not ready after " + timeoutSeconds + "s");
    }


    @Test
    @Tag("HA-TC-51")
    @DisplayName("HA-TC-51")
    public void testBlockingQueueSequentialExecution() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);
        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode());

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("isBlocking", true);

        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode());

        List<String> jobIds = ApiUtils.submitJobs(baseUri1,10, "BlankJob", null);
        assertEquals(10,jobIds.size());

        for (String jobId:jobIds){
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 10, 500));
        }
        JobUtils.validateSequentialJobs(baseUri1, jobIds);
    }

    //Bucketing Queue Tests
    @Test
    @Order(1)
    @Tag("BQ-TC-01")
    @DisplayName("BQ-TC-01: Start bucket job executor with withExecuteWithBuckets=true")
    public void testStartBucketExecutor() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);

        int statusCode = ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode();
        assertEquals(200, statusCode, "Bucket executor failed to start");
        System.out.println("Bucket executor started successfully");
    }


    @Test
    @Order(2)
    @Tag("BQ-TC-02")
    @DisplayName("BQ-TC-02: Add a single job to a new bucket")
    public void testAddJobToNewBucket() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        // Pass bucket-name — if bucket doesn't exist it will be created
        Map<String, Object> params = new HashMap<>();
        params.put("bucket-name", "bucketA");

        List<String> jobIds = ApiUtils.submitJobs(baseUri1, 1, "BlankJob", params);
        assertEquals(1, jobIds.size(), "Job submission to new bucket failed");
        System.out.println("Job submitted to new bucket 'bucketA': " + jobIds.get(0));

        // Verify job completes successfully
        assertTrue(ApiUtils.waitForJobState(baseUri1, jobIds.get(0), "SUCCESSFUL", 10, 1000),
                "Job in bucketA did not complete");
        ApiUtils.validateJobState(baseUri1, jobIds.get(0), "SUCCESSFUL");
    }


    @Test
    @Order(3)
    @Tag("BQ-TC-03")
    @DisplayName("BQ-TC-03: Add multiple jobs to the same bucket")
    public void testAddMultipleJobsToSameBucket() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        // Submit 5 jobs all into the same bucket
        Map<String, Object> params = new HashMap<>();
        params.put("bucket-name", "bucketA");

        List<String> jobIds = ApiUtils.submitJobs(baseUri1, 5, "BlankJob", params);
        assertEquals(5, jobIds.size(), "Not all jobs were submitted to bucketA");
        System.out.println("Submitted 5 jobs to 'bucketA': " + jobIds);

        // All jobs should complete successfully
        for (String jobId : jobIds) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 15, 1000),
                    "Job " + jobId + " in bucketA did not complete");
            ApiUtils.validateJobState(baseUri1, jobId, "SUCCESSFUL");
        }
        System.out.println("All 5 jobs in bucketA completed successfully");
    }


    @Test
    @Order(4)
    @Tag("BQ-TC-04")
    @DisplayName("BQ-TC-04: Add jobs to multiple different buckets")
    public void testAddJobsToMultipleBuckets() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        // Submit jobs into 3 different buckets
        Map<String, Object> paramsA = new HashMap<>();
        paramsA.put("bucket-name", "bucketA");
        Map<String, Object> paramsB = new HashMap<>();
        paramsB.put("bucket-name", "bucketB");
        Map<String, Object> paramsC = new HashMap<>();
        paramsC.put("bucket-name", "bucketC");

        List<String> jobIdsA = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsA);
        List<String> jobIdsB = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsB);
        List<String> jobIdsC = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsC);

        assertEquals(3, jobIdsA.size(), "bucketA job submission count mismatch");
        assertEquals(3, jobIdsB.size(), "bucketB job submission count mismatch");
        assertEquals(3, jobIdsC.size(), "bucketC job submission count mismatch");
        System.out.println("Submitted 3 jobs each to bucketA, bucketB, bucketC");

        // All jobs across all buckets should complete
        List<String> allJobIds = new ArrayList<>();
        allJobIds.addAll(jobIdsA);
        allJobIds.addAll(jobIdsB);
        allJobIds.addAll(jobIdsC);

        for (String jobId : allJobIds) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 10, 1000),
                    "Job " + jobId + " did not complete");
            ApiUtils.validateJobState(baseUri1, jobId, "SUCCESSFUL");
        }
        System.out.println("All jobs across bucketA, bucketB, bucketC completed successfully");
    }


    @Test
    @Order(5)
    @Tag("BQ-TC-05")
    @DisplayName("BQ-TC-05: Validate round robin execution across 2 buckets")
    public void testRoundRobinTwoBuckets() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        // thread-pool-size=1 so only 1 job runs at a time — makes round robin order deterministic
        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 1);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        Map<String, Object> paramsA = new HashMap<>();
        paramsA.put("bucket-name", "bucketA");
        Map<String, Object> paramsB = new HashMap<>();
        paramsB.put("bucket-name", "bucketB");

        // Submit equal jobs to both buckets so round robin is clean: A,B,A,B,A,B
        List<String> jobIdsA = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsA);
        List<String> jobIdsB = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsB);

        assertEquals(3, jobIdsA.size(), "bucketA submission count mismatch");
        assertEquals(3, jobIdsB.size(), "bucketB submission count mismatch");

        // Wait for all jobs to complete before validating order
        for (String jobId : jobIdsA) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 20, 1000),
                    "Job " + jobId + " in bucketA did not complete");
        }
        for (String jobId : jobIdsB) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 20, 1000),
                    "Job " + jobId + " in bucketB did not complete");
        }

        // Validate round robin: execution order should be A,B,A,B,A,B by startTime
        JobUtils.validateRoundRobin(baseUri1,
                List.of(jobIdsA, jobIdsB),
                List.of("bucketA", "bucketB"));
    }


    @Test
    @Order(6)
    @Tag("BQ-TC-06")
    @DisplayName("BQ-TC-06: Validate round robin execution across 3 buckets")
    public void testRoundRobinThreeBuckets() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 1);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        Map<String, Object> paramsA = new HashMap<>();
        paramsA.put("bucket-name", "bucketA");
        Map<String, Object> paramsB = new HashMap<>();
        paramsB.put("bucket-name", "bucketB");
        Map<String, Object> paramsC = new HashMap<>();
        paramsC.put("bucket-name", "bucketC");

        // Submit equal jobs to all 3 buckets — round robin: A,B,C,A,B,C,A,B,C
        List<String> jobIdsA = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsA);
        List<String> jobIdsB = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsB);
        List<String> jobIdsC = ApiUtils.submitJobs(baseUri1, 3, "BlankJob", paramsC);

        assertEquals(3, jobIdsA.size(), "bucketA submission count mismatch");
        assertEquals(3, jobIdsB.size(), "bucketB submission count mismatch");
        assertEquals(3, jobIdsC.size(), "bucketC submission count mismatch");

        // Wait for all jobs to complete before validating order
        for (String jobId : jobIdsA) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 30, 1000),
                    "Job " + jobId + " in bucketA did not complete");
        }
        for (String jobId : jobIdsB) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 30, 1000),
                    "Job " + jobId + " in bucketB did not complete");
        }
        for (String jobId : jobIdsC) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 30, 1000),
                    "Job " + jobId + " in bucketC did not complete");
        }

        // Validate round robin: A,B,C,A,B,C,A,B,C
        JobUtils.validateRoundRobin(baseUri1,
                List.of(jobIdsA, jobIdsB, jobIdsC),
                List.of("bucketA", "bucketB", "bucketC"));
    }


    @Test
    @Order(7)
    @Tag("BQ-TC-07")
    @DisplayName("BQ-TC-07: Large number of jobs in a single bucket")
    public void testLargeNumberOfJobsInSingleBucket() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        Map<String, Object> params = new HashMap<>();
        params.put("bucket-name", "bucketA");

        // Submit 50 jobs into the same bucket
        List<String> jobIds = ApiUtils.submitJobs(baseUri1, 50, "BlankJob", params);
        assertEquals(50, jobIds.size(), "Not all 50 jobs were submitted to bucketA");
        System.out.println("Submitted 50 jobs to 'bucketA'");

        // All 50 should complete successfully
        for (String jobId : jobIds) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 30, 1000),
                    "Job " + jobId + " did not complete");
            ApiUtils.validateJobState(baseUri1, jobId, "SUCCESSFUL");
        }
        System.out.println("All 50 jobs in bucketA completed successfully");
    }


    @Test
    @Order(8)
    @Tag("BQ-TC-08")
    @DisplayName("BQ-TC-08: Large number of buckets — all jobs should be submitted and successful")
    public void testLargeNumberOfBuckets() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Bucket executor failed to start");

        // Create 20 buckets, submit 3 jobs to each = 60 jobs total
        int bucketCount = 20;
        int jobsPerBucket = 3;
        List<String> allJobIds = new ArrayList<>();

        for (int i = 1; i <= bucketCount; i++) {
            Map<String, Object> params = new HashMap<>();
            params.put("bucket-name", "bucket-" + i);

            List<String> jobIds = ApiUtils.submitJobs(baseUri1, jobsPerBucket, "BlankJob", params);
            assertEquals(jobsPerBucket, jobIds.size(),
                    "Job submission count mismatch for bucket-" + i);
            allJobIds.addAll(jobIds);
            System.out.println("Submitted " + jobsPerBucket + " jobs to bucket-" + i);
        }

        assertEquals(bucketCount * jobsPerBucket, allJobIds.size(),
                "Total submitted job count mismatch");
        System.out.println("All " + allJobIds.size() + " jobs submitted across " + bucketCount + " buckets");

        // Every single job across all buckets must complete successfully
        for (String jobId : allJobIds) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL", 30, 1000),
                    "Job " + jobId + " did not complete");
            ApiUtils.validateJobState(baseUri1, jobId, "SUCCESSFUL");
        }
        System.out.println("All " + allJobIds.size() + " jobs across " + bucketCount + " buckets completed successfully");
    }

    @Test
    @Order(9)
    @Tag("BQ-TC-09")
    @DisplayName("BQ-TC-09: Validate withMaxParallelJobsPerBucket=2 limits concurrency within a bucket")
    public void testMaxParallelJobsPerBucket() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        // withMaxParallelJobsPerBucket=2 means at most 2 jobs from the same bucket run at once
        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);  // large pool so the bucket limit is the bottleneck
        config.put("withExecuteWithBuckets", true);
        config.put("withMaxParallelJobsPerBucket", 2);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Executor failed to configure");

        Map<String, Object> params = new HashMap<>();
        params.put("bucket-name", "bucketA");

        // Submit 6 jobs — if parallelism is correctly capped at 2,
        // at no point should more than 2 jobs overlap in their start/stop window
        List<String> jobIds = ApiUtils.submitJobs(baseUri1, 6, "BlankJob", params);
        assertEquals(6, jobIds.size(), "Job submission count mismatch");
        System.out.println("Submitted 6 jobs to bucketA with maxParallelJobsPerBucket=2");

        // Wait for all jobs to finish before inspecting timings
        for (String jobId : jobIds) {
            assertTrue(ApiUtils.waitForJobState(baseUri1, jobId, "SUCCESSFUL",30, 1000),
                    "Job " + jobId + " did not complete");
        }

        // Validate that no more than 2 jobs ran concurrently at any point
        JobUtils.validateMaxParallelism(baseUri1, jobIds, 2);
    }


    @Test
    @Order(10)
    @Tag("BQ-TC-10")
    @DisplayName("BQ-TC-10: Bucket blocks on job failure, other buckets unaffected, unblock resumes execution")
    public void testBucketBlocksOnJobFailure() throws Exception {
        List<String> podUrls = scaleAndGetPodUrls(1);
        String baseUri1 = podUrls.get(0);

        assertEquals(200, OrchestratorAPI.startOrchestrator(baseUri1).getStatusCode(),
                "Orchestrator failed to start");

        Map<String, Object> config = new HashMap<>();
        config.put("thread-pool-size", 10);
        config.put("withExecuteWithBuckets", true);
        assertEquals(200, ExecutorAPI.configureExecutor(baseUri1, config).getStatusCode(),
                "Executor failed to configure");

        // Step 1: Submit a failing job to bucketA — this should cause bucketA to block
        Map<String, Object> bucketAParams = new HashMap<>();
        bucketAParams.put("bucket-name", "bucketA");

        List<String> failingJobIds = ApiUtils.submitJobs(baseUri1, 1, "FailOnceJob", bucketAParams);
        String failingJobId = failingJobIds.get(0);
        System.out.println("Submitted FailOnceJob to bucketA: " + failingJobId);

        // Step 2: Confirm the failing job reaches FAILED state
        assertTrue(ApiUtils.waitForJobState(baseUri1, failingJobId, "FAILED", 15, 1000),
                "FailOnceJob did not reach FAILED state");
        System.out.println("FailOnceJob " + failingJobId + " confirmed FAILED — bucketA should now be blocked");

        // Step 3: Submit a normal job to the same blocked bucketA
        // Expectation: it queues but cannot run — stays in READY_TO_RUN
        List<String> blockedJobIds = ApiUtils.submitJobs(baseUri1, 1, "BlankJob", bucketAParams);
        String blockedJobId = blockedJobIds.get(0);
        System.out.println("Submitted BlankJob to blocked bucketA: " + blockedJobId);

        assertTrue(ApiUtils.waitForJobState(baseUri1, blockedJobId, "READY_TO_RUN", 10, 500),
                "Job in blocked bucketA should be in READY_TO_RUN state but wasn't");
        System.out.println("Confirmed: job " + blockedJobId + " is stuck in READY_TO_RUN — bucket is blocked");

        // Step 4: Submit a normal job to a different bucket — should be completely unaffected
        Map<String, Object> bucketBParams = new HashMap<>();
        bucketBParams.put("bucket-name", "bucketB");

        List<String> unaffectedJobIds = ApiUtils.submitJobs(baseUri1, 1, "BlankJob", bucketBParams);
        String unaffectedJobId = unaffectedJobIds.get(0);
        System.out.println("Submitted BlankJob to unaffected bucketB: " + unaffectedJobId);

        assertTrue(ApiUtils.waitForJobState(baseUri1, unaffectedJobId, "SUCCESSFUL", 15, 1000),
                "Job in bucketB should complete successfully regardless of bucketA being blocked");
        System.out.println("Confirmed: bucketB job " + unaffectedJobId + " completed — bucket isolation working");

        // Step 5: Unblock the executor — bucketA should resume
        // Note: unblockExecutor acts globally; if your API supports per-bucket unblocking,
        // pass bucketA name here instead
        assertEquals(200, ExecutorAPI.unblockExecutor(baseUri1).getStatusCode(),
                "Unblock executor call failed");
        System.out.println("Executor unblocked — bucketA should now resume");

        // Step 6: The previously stuck job should now run to completion
        assertTrue(ApiUtils.waitForJobState(baseUri1, blockedJobId, "SUCCESSFUL", 15, 1000),
                "Job " + blockedJobId + " did not complete after bucket was unblocked");
        System.out.println("Confirmed: previously blocked job " + blockedJobId
                + " completed successfully after unblock");

        // Step 7: Sanity check — the original failing job's state should still be FAILED
        assertEquals("FAILED", ApiUtils.getJobState(baseUri1, failingJobId),
                "FailOnceJob state should remain FAILED after unblock");
        System.out.println("Confirmed: FailOnceJob " + failingJobId + " state is still FAILED as expected");
    }

}