package zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Description 服务器端向Zookeeper注册代码
 * @Author talent2333
 * @Date 2020/5/29 21:17
 */
public class DistributeServer {

    private String connectionStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 100000;
    private ZooKeeper zk;
    private static String parentNode = "/servers";
    private static String hostname = "hadoop101";

    public static void main(String[] args) throws Exception {

        DistributeServer server = new DistributeServer();
        //连接服务器
        server.connect();
        //注册子节点
        server.registerServer(hostname);
        //漫长的等待
        server.business(hostname);

    }

    //创建连接
    private void connect() throws IOException {
        zk = new ZooKeeper(connectionStr, sessionTimeOut, event -> {
        });
    }
    @Deprecated
    //关闭连接
    public void close() throws InterruptedException {
        zk.close();
    }

    //注册服务器
    private void registerServer(String hostname) throws KeeperException, InterruptedException {

        String node = zk.create(parentNode + "/server1",
                hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        System.out.println(hostname + " is online " + node);
    }

    //业务功能
    private void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

}
