package org.apache.phoenix.service;

/**
 * Created by rshrivastava on 3/1/17.
 */
public class ZookeeperNodeData implements ServiceInstance {

    @Override
    public int getServerLoad() {
        return 0;
    }

    @Override
    public String getServiceHost() {
        return null;
    }

    @Override
    public Integer getServicePort() {
        return null;
    }

    @Override
    public ServiceInstance getServiceData() {
        return null;
    }
}
