package org.apache.phoenix.service;

import java.io.IOException;

/**
 * Created by rshrivastava on 3/1/17.
 */
public interface ServiceDiscoverer {

    public ServiceInstance getService();
    public  void start() throws Exception;
    public void close() throws  Exception;
}
