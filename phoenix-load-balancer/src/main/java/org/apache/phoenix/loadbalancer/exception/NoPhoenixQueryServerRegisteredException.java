package org.apache.phoenix.loadbalancer.exception;

/**
 * Created by rshrivastava on 3/20/17.
 */
public class NoPhoenixQueryServerRegisteredException extends Exception{

    public NoPhoenixQueryServerRegisteredException(String st){
        super(st);
    }

    public NoPhoenixQueryServerRegisteredException(){
        super();
    }
}
