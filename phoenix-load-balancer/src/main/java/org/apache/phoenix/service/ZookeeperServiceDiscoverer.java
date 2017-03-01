package org.apache.phoenix.service;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Created by rshrivastava on 3/1/17.
 */
public class ZookeeperServiceDiscoverer implements ServiceDiscoverer {

    private static final String BASE_PATH = "/services";

    private CuratorFramework curatorClient;

    private ServiceDiscovery serviceDiscovery;

    private ServiceProvider<Service> serviceProvider;

    private JsonInstanceSerializer<Service> serializer;

    private final List<Closeable> closeAbles = Lists.newArrayList();

    private String zookeeperAddress;

    private String serviceName;

    private Service service;

    public ZookeeperServiceDiscoverer() {

        curatorClient = CuratorFrameworkFactory.newClient(zookeeperAddress,
                new ExponentialBackoffRetry(1000, 3));

        serializer = new JsonInstanceSerializer<>(ZookeeperService.class); // Payload Serializer

        serviceDiscovery = ServiceDiscoveryBuilder.builder(Service.class).client(curatorClient)
                .basePath(BASE_PATH).serializer(serializer).build(); // Service Discovery

        serviceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(serviceName).build();
    }

    @Override
    public Service getService() {
        try {
            return  serviceProvider.getInstance().getPayload();
        }
        catch (Exception e) {
            throw new RuntimeException("Error obtaining service url", e);
        }
    }

    @Override
    public void start() throws Exception {
        curatorClient.start();
        closeAbles.add(curatorClient);
        serviceDiscovery.start();
        closeAbles.add(0, serviceDiscovery);
        serviceProvider.start();
        closeAbles.add(0, serviceProvider);
    }

    @Override
    public void close() throws IOException {
        for (Closeable closeable : closeAbles) {
            closeable.close();
        }
    }
}
