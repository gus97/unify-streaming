package cn.migu.streaming.kafka;

import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * kafka操作工具类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public final class KafkaUtils
{

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static final int ZK_TIMEOUT_MSEC = (int)TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

    private KafkaUtils()
    {
    }

    /**
     * 创建topic
     * @param zkServers zookeeper集群连接地址
     * @param topic topic名称
     * @param partitions partition数量
     */
    public static void maybeCreateTopic(String zkServers, String topic, int partitions)
    {
        maybeCreateTopic(zkServers, topic, partitions, new Properties());
    }

    /**
     * 创建topic
     * @param zkServers zookeeper集群连接地址
     * @param topic topic名称
     * @param partitions partition数量
     * @param topicProperties topic属性选项
     */
    public static void maybeCreateTopic(String zkServers, String topic, int partitions, Properties topicProperties)
    {
        try (AutoZkClient zkClient = new AutoZkClient(zkServers))
        {
            if (AdminUtils.topicExists(zkClient, topic))
            {
                log.info("No need to create topic {} as it already exists", topic);
            }
            else
            {
                log.info("Creating topic {}", topic);
                try
                {
                    AdminUtils.createTopic(zkClient, topic, partitions, 1, topicProperties);
                    log.info("Created Zookeeper topic {}", topic);
                }
                catch (TopicExistsException tee)
                {
                    log.info("Zookeeper topic {} already exists", topic);
                }
            }
        }
    }

    /**
     * @param zkServers zookeeper集群连接地址
     * @param topic topic名称
     * @return topic存在返回true,不存在返回false
     */
    public static boolean topicExists(String zkServers, String topic)
    {
//        try (AutoZkClient zkClient = new AutoZkClient(zkServers))
//        {
//            return AdminUtils.topicExists(zkClient, topic);
//        }

        return true;
    }

    /**
     * 删除topic
     * @param zkServers zookeeper集群连接地址
     * @param topic topic名称
     */
    public static void deleteTopic(String zkServers, String topic)
    {
        try (AutoZkClient zkClient = new AutoZkClient(zkServers))
        {
            if (AdminUtils.topicExists(zkClient, topic))
            {
                log.info("Deleting topic {}", topic);
                try
                {
                    AdminUtils.deleteTopic(zkClient, topic);
                    log.info("Deleted Zookeeper topic {}", topic);
                }
                catch (ZkNodeExistsException nee)
                {
                    log.info("Delete was already scheduled for Zookeeper topic {}", topic);
                }
            }
            else
            {
                log.info("No need to delete topic {} as it does not exist", topic);
            }
        }
    }

    /**
     * @param zkServers zookeeper集群连接地址
     * @param groupID 消费组
     * @param topic topic名称
     * @return topic&partition,offset映射
     */
    public static Map<TopicAndPartition, Long> getOffsets(String zkServers, String groupID, String topic)
    {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
        Map<TopicAndPartition, Long> offsets = new HashMap<>();
        try (AutoZkClient zkClient = new AutoZkClient(zkServers))
        {
            List<Object> partitions = JavaConversions.seqAsJavaList(ZkUtils.getPartitionsForTopics(zkClient,
                    JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
            for (Object partition : partitions)
            {
                String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
                Option<String> maybeOffset = ZkUtils.readDataMaybeNull(zkClient, partitionOffsetPath)._1();
                Long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : null;
                TopicAndPartition topicAndPartition =
                        new TopicAndPartition(topic, Integer.parseInt(partition.toString()));
                offsets.put(topicAndPartition, offset);
            }
        }
        return offsets;
    }

    /**
     * 设置offset
     * @param zkServers zookeeper集群连接地址
     * @param groupID 消费组ID
     * @param offsets topic&partition,offset映射
     */
    public static void setOffsets(String zkServers, String groupID, Map<TopicAndPartition, Long> offsets)
    {
        try (AutoZkClient zkClient = new AutoZkClient(zkServers))
        {
            for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet())
            {
                TopicAndPartition topicAndPartition = entry.getKey();
                ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topicAndPartition.topic());
                int partition = topicAndPartition.partition();
                long offset = entry.getValue();
                String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
                ZkUtils.updatePersistentPath(zkClient, partitionOffsetPath, Long.toString(offset));
            }
        }
    }

    // Just exists for Closeable convenience
    private static final class AutoZkClient extends ZkClient implements Closeable
    {
        AutoZkClient(String zkServers)
        {
            super(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, ZKStringSerializer$.MODULE$);
        }
    }

}
