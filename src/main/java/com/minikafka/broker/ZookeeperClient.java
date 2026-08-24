/*ZookeeperClient: The coordination layer for broker discovery, topic configuration, and leader election. Allow Kafka to focus on message handling*/

package com.simplekafka.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient implements Watcher{
    //observability
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSTION_TIMEOUT = 30000;

    private static final String host;
    private static final int port;
    private ZooKeeper zooKeeper;
    private CountDownLaunch connectedSignal = new countDownLatch(1);
    
    //constructor
    public ZookeeperClient(String host, int port){
        this.host = host;
        this.port = port;
    }

    //Connection management
    public void connect() throws IOException, InterrupedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        //create required paths if not existing
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }

    //get connection string
    public String getConnectString(){
        return host + ":" + port;
    }

    //close the connection
    public void close() throws InterruptedException{
        if (zooKeeper != null){
            zooKeeper.close();
        }
    }

    /*Creating persistent node for topic configurations, partition assignments, persisting consumer group offsets*/
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException{
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null){
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1)
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /*Creating ephemeral (not persistent) nodes for broker registration and tracking*/
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException{
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null){
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        } else{
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    //exists function
    public boolean exists(String path) throws KeeperException, InterruptedException{
        Stat stat = zooKeeper.exists(path, false);
        return stat != null; 
    }

    //return String data
    public String getData(String path) throws KeeperExcpetion, InterruptedException{
        byte[] data = zooKeeper.getData(path, false, null)
        return new String(data);
    }

    //set using String data
    public void setData(String path, String data) throws KeeperException, InterruptedException{
        zooKeeper.setData(path, data.getBytes(), -1)
    }

    public List<String> getChildren(String path) throws KeeperExcpetion, InterruptedException{
        try{
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e){
            return new ArrayList<>();
        }
    }

    //Creating a path recursively
    public void createPath(String path){
        try{
            if (path.equals("/")){
                return;
            }

            //getting the last instance of a slash (directory)
            int lastSlashIndex = path.lastIndexOf('/');
            //if this recursive call isn't with the root directory
            if (lastSlashIndex > 0){
                String parent = substring(0, lastSlashIndex);
                return createPath(parent);
            }
            
            //if there is no node at this path
            if (zooKeeper.exists(path, false) == null){
                zooKeeper.create(path, new byte[0], ZooDef.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }

    //check for changes in child nodes
    public void watchChildren(String path, ChildrenCallback callback){
        //starting from the path given to watch for all children changes
        try{
            //use custom implementation for override of process: event->
            List<String> children = zooKeeper.getChildren(path, event ->{
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                    //actually processing the children recursively
                    try {
                        //need to re-register a watch once you create a watch event once
                        List<String> newChildren = zooKeeper.getChildren(path, event2 ->{
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged){
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e){
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e){
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        } 
    }

    public void watchNode(String path, NodeCallback callback){
        try{
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted){
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged){
                    callback.onNodeChanged();
                } else if (event.getType() == Watcher.Event.EventType.NodeCreated){
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e){
            LOGGER.log(Level.SEVERE, "Failed to watch node for path: " + path, e);
        }
    }

    //deleting a node
    public void deleteNode(String path) throws KeeperException, InterruptedException{
        if (exists(path)){
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted node at path: " + path);
        }
    }

    //processing zookeeper events
    @Override
    public void process(WatchedEvent event){
        if (event.getState() == Event.KeeperState.SyncConnected){
            //use count down to unblock the other threads from running
            connectedSignal.countDown();
            LOGGER.info("ZooKeeper connected");
        } else if (event.getState() == Event.KeeperState.Disconnected){
            LOGGER.warning("Disconnected from ZooKeeper");
        //expired state, restart
        } else if (event.getState() == Event.KeeperState.Expired){
            LOGGER.warning("ZooKeeper session expired, reconnecting... ");
            try{
                if (zooKeeper != null){
                    zooKeeper.close();
                }
                //create new count down latch to block other threads while ZooKeeper object session is being created
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e){
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public interface ChildrenCallback{
        void onChildrenChanged(List<String> children);
    }
    
    public interface NodeCallback{
        void onNodeChanged();
    }


}