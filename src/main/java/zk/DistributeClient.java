package zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description 客户端
 * @Author talent2333
 * @Date 2020/5/29 21:33
 */
public class DistributeClient {

    private String connectionStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 100000;
    private ZooKeeper zk;
    private String parentNode = "/servers";

    public static void main(String[] args) throws Exception {

        DistributeClient client = new DistributeClient();
        //连接服务器
        client.connect();
        //获取服务器列表
        client.getServerList();
        //漫长的等待
        client.business();
    }

    //创建连接
    private void connect() throws IOException {
        zk = new ZooKeeper(connectionStr, sessionTimeOut, event -> {
            //再次启动监听
            try {
                getServerList();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    //获取服务器列表
    private void getServerList() throws KeeperException, InterruptedException {

        List<String> children = zk.getChildren(parentNode, true);
        //服务器列表
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child,
                    false, null);
            servers.add(new String(data));
        }
        //打印服务器列表
        System.out.println("========华丽的分割线========");
        servers.forEach(System.out::println);
    }

    //业务功能
    private void business() throws InterruptedException {
        System.out.println("Client is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

}
