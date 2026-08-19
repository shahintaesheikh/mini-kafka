//Holds information about a broker in each cluster
package com.minikafka.broker;

public class BrokerInfo{
    private final int id;
    private final String host; 
    private final int port;

    //constructor
    public BrokerInfo(int id, String host, int port){
        //set member variables
        this.id = id;
        this.host = host;
        this.port = port;
    }

    //id getter
    public int getId(){
        return id; 
    }

    //host getter
    public int getHost(){
        return host; 
    }

    //port getter
    public int getPort(){
        return port; 
    }

    //string casting, overrides Object class method
    @Override
    public String toString(){
        return "BrokerInfo{id= " + id + ", host= " + host + ", port= " + port + "}";
    }

    //equals operator, overrides default
    @Override
    public boolean equals(Object obj){
        if(this == obj) return true;
        //if the object doesn't exist or the object class isn't BrokerInfo
        if(obj == null || getClass() != obj.getClass()) return false;

        BrokerInfo other = (BrokerInfo) obj;
        return id == other.id;
    }

    @Override
    public int hashCode(){
        return Integer.hashCode(id);
    }
}