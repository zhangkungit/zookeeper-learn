package com.zhangkun.leaderselector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;

public class WorkServer {

    private volatile boolean running = false;

    private ZkClient zkClient;

    private static final String MASTER_PATH = "/master";

    private IZkDataListener dataListener;

    //servers节点信息
    private RunningData serverData;
    //master节点信息
    private RunningData masterData;

    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
    private int delayTime = 5;

    public WorkServer(RunningData rd) {
        this.serverData = rd;
        this.dataListener = new IZkDataListener() {

            public void handleDataDeleted(String dataPath) throws Exception {
                //case1: 直接参与选举
                takeMaster();

                //case2: 兼容网络抖动master假死的情况
                /**
                 * 1.如果之前master是自己，则直接选举
                 * 2.不是自己，等一下，让之前的master优先参与选举
                 */
                /*if (masterData != null && masterData.getName().equals(serverData.getName())) {
                    takeMaster();
                } else {
                    delayExector.schedule(new Runnable() {
                        public void run() {
                            takeMaster();
                        }
                    }, delayTime, TimeUnit.SECONDS);
                }*/
            }

            public void handleDataChange(String dataPath, Object data)
                    throws Exception {
                // TODO Auto-generated method stub

            }
        };
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * 启动WorkServer节点
     * @throws Exception
     */
    public void start() throws Exception {
        if (running) {
            throw new Exception("server has startup...");
        }
        running = true;
        //向zk注册master节点的变动监听
        zkClient.subscribeDataChanges(MASTER_PATH, dataListener);
        takeMaster();
    }

    public void stop() throws Exception {
        if (!running) {
            throw new Exception("server has stoped");
        }
        running = false;

        delayExector.shutdown();
        zkClient.unsubscribeDataChanges(MASTER_PATH, dataListener);
        releaseMaster();

    }

    //参与选举
    private void takeMaster() {
        if (!running) {
            return;
        }

        try {
            /**
             * 尝试选举自己
             * 1.选举成功
             * 2.选举失败，有ZkNodeExistsException，则表示已经有master节点，则读取zk中的master信息
             */
            zkClient.create(MASTER_PATH, serverData, CreateMode.EPHEMERAL);
            masterData = serverData;
            System.out.println(serverData.getName() + " is master");

            //模拟网络抖动情形
            delayExector.schedule(new Runnable() {
                @Override
                public void run() {
                    if (checkMaster()) {
                        releaseMaster();
                    }
                }
            }, 5, TimeUnit.SECONDS);

        } catch (ZkNodeExistsException e) {
            RunningData runningData = zkClient.readData(MASTER_PATH, true);
            if (runningData == null) {
                takeMaster();
            } else {
                masterData = runningData;
            }
        } catch (Exception e) {
            // ignore;
        }

    }

    private void releaseMaster() {
        if (checkMaster()) {
            zkClient.delete(MASTER_PATH);

        }

    }

    //检查自己是否是master
    private boolean checkMaster() {
        try {
            RunningData eventData = zkClient.readData(MASTER_PATH);
            masterData = eventData;
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
            return false;
        } catch (ZkNoNodeException e) {
            return false;
        } catch (ZkInterruptedException e) {
            return checkMaster();
        } catch (ZkException e) {
            return false;
        }
    }

}
