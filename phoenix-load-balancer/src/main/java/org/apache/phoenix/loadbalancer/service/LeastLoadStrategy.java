package org.apache.phoenix.loadbalancer.service;

import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceProvider;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Created by rshrivastava on 3/4/17.
 */
public class LeastLoadStrategy implements ProviderStrategy<Instance> {


    private List<ServiceInstance<Instance>> instances;

    @Override
    public ServiceInstance<Instance> getInstance(InstanceProvider<Instance> instanceProvider) throws Exception {
        instances = instanceProvider.getInstances();
        if ( instances.size() == 0 )
        {
            return null;
        };
        Collections.sort(instances, new Comparator<ServiceInstance<Instance>>() {
            @Override
            public int compare(ServiceInstance<Instance> o1, ServiceInstance<Instance> o2) {
                return o1.getPayload().compareTo(o2.getPayload());
            }
        });
        return instances.get(0);
    }

}