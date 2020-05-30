package zk;

import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.transform.Result;
import java.io.IOException;
import java.util.List;

/**
 * @Description
 * @Author talent2333
 * @Date 2020/5/29 16:21
 */
public class TestZk {

    private String connectionStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 100000;
    private ZooKeeper zk;

    @Before
    public void init() throws IOException {
        zk = new ZooKeeper(connectionStr, sessionTimeOut
                , new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {}
        });
    }

    @After
    public void close() throws InterruptedException {
        zk.close();
    }

    /**
     * 获取子节点
     * 监听节点
     *
     * @throws IOException
     */
    @Test
    public void getChild() throws Exception {

//        zk.getChildren(path,watcher)
//        zk.getChildren(path,true/false)
        List<String> children = zk.getChildren("/xty", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("监听到节点变化");
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
        //不能关闭客户端，否则无法监听到节点变化
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 创建节点
     * 参数1:节点名称 参数2:节点数据
     * 参数3:权限控制 参数4:节点类型(1.带序列 2.持久性)
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {

        String newNode = zk.create("/xty/id", "very slow".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(newNode);
    }

    /**
     * 获取节点数据
     */
    @Test
    public void getData() throws KeeperException, InterruptedException {

        Stat stat = getStat("/xty");
        if (stat == null) {
            System.out.println("xty 不存在");
            return;
        }
        System.out.println("节点存在");
        byte[] data = zk.getData("/xty", false, stat);
        System.out.println("data = " + new String(data));

    }

    //获取节点状态
    private Stat getStat(String path) throws KeeperException, InterruptedException {
        return zk.exists(path, false);
    }

    /**
     * 节点是否存在
     *
     * @param path 访问路径
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void existNode() throws KeeperException, InterruptedException {

        Stat stat = zk.exists("/xty12", false);
        if (stat == null) {
            System.out.println("节点不存在");
        } else {
            System.out.println("节点存在");
        }
    }

    /**
     * 设置节点数据
     */
    @Test
    public void setData() throws KeeperException, InterruptedException {

        Stat stat = getStat("/xty");
        if (stat == null) {
            System.out.println("节点不存在");
            return;
        }
//        Stat resultStat = zk.setData("/xty", "xitianyu".getBytes(), stat.getVersion());
        Stat resultStat = zk.setData("/xty", "phone".getBytes(), -1);
    }

    /**
     * 删除节点-递归方法
     *
     * @param path 递归路径
     * @param zk   服务器对象
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void delNodes(String path, ZooKeeper zk) throws KeeperException, InterruptedException {

        List<String> children = zk.getChildren(path, false);
        if (children.size() == 0) {
            zk.delete(path, -1);
        } else {
            for (String child : children) {
                delNodes(path + "/" + child, zk);
            }
            zk.delete(path, -1);
        }
    }

    /**
     * 删除节点
     */
    @Test
    public void del() throws KeeperException, InterruptedException {

        delNodes("/a", zk);
    }
}
