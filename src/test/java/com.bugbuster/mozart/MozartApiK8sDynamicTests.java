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
            assertTrue(ApiUtils.waitForJobCompletion(jobId, 10, 500));
        }
        JobUtils.validateSequentialJobs(jobIds);
    }

}