package com.bugbuster.utils;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;

import java.util.List;
import java.util.stream.Collectors;

public class K8sHelper {
    private static final String NAMESPACE = "mozart";
    private static final String DEPLOYMENT_NAME = "server-app";
    private String baseUri = "http://server-app.local";

    public static void scaleAndGetPodUrls(int replicas) throws InterruptedException {
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
        }
    }

    private static void waitForPodsReady(KubernetesClient client, int replicas, int timeoutSeconds) throws InterruptedException {
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

    public static void scaleDownPodsToZero() throws InterruptedException{
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            RollableScalableResource deployment =
                    client.apps()
                            .deployments()
                            .inNamespace(NAMESPACE)
                            .withName(DEPLOYMENT_NAME);
            deployment.scale(0);

            System.out.println("Scaled deployment down to 0 replicas.");

            Thread.sleep(10000);
        }
    }
}
