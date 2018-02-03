package cn.migu.streaming.spark;

import cn.migu.streaming.common.ClassUtils;
import cn.migu.streaming.common.Functions;
import cn.migu.streaming.db.DataTraceLog;
import cn.migu.streaming.db.OracleDialect;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DataTraceModel;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * spark层抽象基类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public abstract class AbstractSparkLayer<K, M> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger("streaming");

    public static JavaSparkContext SJSC;

    private final Config config;

    private final String streamingMaster;

    private final String inputTopic;

    private final String inputTopicLockMaster;

    private final String inputBroker;

    private final String groupId;

    /*private final String updateTopic;

    private final String updateTopicLockMaster;*/

    private final Class<K> keyClass;

    private final Class<M> messageClass;

    private final Class<? extends Decoder<K>> keyDecoderClass;

    private final Class<? extends Decoder<M>> messageDecoderClass;

    protected final int generationIntervalSec;

    private final Map<String, Object> extraSparkConfig;

    @SuppressWarnings("unchecked")
    protected AbstractSparkLayer(Config config, DataCache dataCache) {
        Objects.requireNonNull(config);
        this.config = config;
        this.streamingMaster = dataCache.getJarConfParam().getSparkMaster();
        this.inputTopic = dataCache.getKafkaConf().getTopic();
        this.inputTopicLockMaster = dataCache.getKafkaConf().getZkconnect();
        this.inputBroker = dataCache.getKafkaConf().getBrokers();
        this.groupId = dataCache.getKafkaConf().getUsername();
        //this.updateTopic = ConfigUtils.getOptionalString(config, "update-topic");
        //this.updateTopicLockMaster = ConfigUtils.getOptionalString(config, "update-topic-lock-master");
        this.keyClass = ClassUtils.loadClass(config.getString("key-class"));
        this.messageClass = ClassUtils.loadClass(config.getString("message-class"));
        this.keyDecoderClass =
                (Class<? extends Decoder<K>>) ClassUtils.loadClass(config.getString("key-decoder-class"), Decoder.class);
        this.messageDecoderClass =
                (Class<? extends Decoder<M>>) ClassUtils.loadClass(config.getString("message-decoder-class"), Decoder.class);
        this.generationIntervalSec =
                Integer.valueOf(dataCache.getJarConfParam().getDuration());//config.getInt("generation-interval-sec");

        this.extraSparkConfig = new HashMap<>();
        for (Map.Entry<String, ConfigValue> e : config.getConfig("default-streaming-config").entrySet()) {
            extraSparkConfig.put(e.getKey(), e.getValue().unwrapped());
        }

        Preconditions.checkArgument(generationIntervalSec > 0);
    }

    /**
     * @return streaming jar名称
     */
    protected abstract String getLayerName();

    protected final Config getConfig() {
        return config;
    }

    protected final String getGroupID() {
        return this.groupId;
    }

    protected final String getInputTopicLockMaster() {
        return inputTopicLockMaster;
    }

    protected final Class<K> getKeyClass() {
        return keyClass;
    }

    protected final Class<M> getMessageClass() {
        return messageClass;
    }

    protected final JavaStreamingContext buildStreamingContext(DataCache dataCache) {
        log.info("Starting SparkStreamingContext with interval {} seconds", generationIntervalSec);

        SparkConf sparkConf = new SparkConf(true);

        sparkConf.setMaster(streamingMaster);
        sparkConf.setAppName(getLayerName());

        /*for (Map.Entry<String, ?> e : extraSparkConfig.entrySet())
        {
            sparkConf.setIfMissing(e.getKey(), e.getValue().toString());
        }*/
        sparkConf.set("spark.scheduler.mode", "FAIR");
        List<String> jars = this.config.getStringList("jars");

        sparkConf.setJars(jars.toArray(new String[]{}));

        sparkConf.set("spark.streaming.kafka.maxRatePerPartition",
                String.valueOf(dataCache.getJarConfParam().getMaxRatePerPartition()));

        if (StringUtils.isNotEmpty(dataCache.getJarConfParam().getTotalCores())) {
            sparkConf.set("spark.cores.max", dataCache.getJarConfParam().getTotalCores());
        }

        if (StringUtils.isNotEmpty(dataCache.getJarConfParam().getExecutorMemory())) {
            sparkConf.set("spark.executor.memory", dataCache.getJarConfParam().getExecutorMemory() + "M");
        }

        //本地测试
        //sparkConf.set("spark.sql.hive.thriftServer.singleSession", "true");
        //System.setProperty("hive.metastore.uris","thrift://192.168.129.186:9083");

        // Turn this down to prevent long blocking at shutdown
        sparkConf.setIfMissing("spark.streaming.gracefulStopTimeout",
                Long.toString(TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS)));
        sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
        long generationIntervalMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);

        String hdfsPath = this.config.getString("hdfs-addr");
        sparkConf.set("spark.eventLog.dir", hdfsPath + "/user/spark/applicationHistory");
        sparkConf.set("spark.eventLog.enabled", "true");
        sparkConf.set("spark.port.maxRetries", "25");

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));

        SJSC = jsc;
        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();
        //dataCache.setSqlContext(new SQLContext(jsc));
        //HiveContext hiveContext = new HiveContext(jsc);
        //hiveContext.sql("with tt as (select a.device_id, a.device_number, a.gcid, a.data_type, a.videotype, sum(case when opertype = 8 then 10 when opertype = 13 then 7 when opertype = 15 then 7 when opertype = 12 then 7 when opertype = 7 then 5 when opertype = 10 then 5 when opertype = 9 then 5 when opertype = 4 then 1 when opertype = 2 then 0 when opertype = 16 then 7 when opertype = 17 then -7 when opertype = 18 then -7 when opertype = 19 then 7 end) score from migu_zone.ods_content_user_oper a where batch_no =from_unixtime(unix_timestamp(), 'yyyyMMdd') and in_time between from_unixtime(unix_timestamp() - 3600) and from_unixtime(unix_timestamp()) and (device_id = '13805196147' or device_id = '13888888888' or device_id = '13999999999') group by a.device_id, a.device_number, a.gcid, a.data_type, a.videotype), tt1 as (select b.device_id, b.device_number, b.gcid, b.data_type, b.videotype, d.gcid gcid1, d.data_type data_type1, d.videotype videotype1, d.score, row_number() over(partition by b.device_id, b.device_number, b.gcid order by d.score desc) rn from tt b join tt c on (b.gcid = c.gcid and b.device_id <> c.device_id ) join tt d on (c.device_id = d.device_id and c.gcid <> d.gcid and d.data_type=b.data_type)) insert overwrite table migu_zone.ods_content_hot_user partition(batch_no) select device_id, device_number, gcid, data_type, videotype, gcid1, data_type1, videotype1, score, current_timestamp, from_unixtime(unix_timestamp(), 'yyyyMMddHHmm') from tt1 where rn <= 5");
        //dataCache.setHiveContext(hiveContext);
        dataCache.setSparkSession(sparkSession);
        OracleDialect.init();

        return new JavaStreamingContext(jsc, new Duration(generationIntervalMS));
    }

    /**
     * 创建输入DStream
     *
     * @param streamingContext streaming context对象
     */
    protected final JavaInputDStream<MessageAndMetadata<K, M>> buildInputDStream(JavaStreamingContext streamingContext,
                                                                                 DataCache dataCache) {

        Preconditions.checkArgument(cn.migu.streaming.kafka.KafkaUtils.topicExists(inputTopicLockMaster, inputTopic),
                "Topic %s does not exist; did you create it?",
                inputTopic);
        /*if (updateTopic != null && updateTopicLockMaster != null)
        {
            Preconditions.checkArgument(cn.migu.streaming.kafka.KafkaUtils.topicExists(updateTopicLockMaster,
                updateTopic), "Topic %s does not exist; did you create it?", updateTopic);
        }*/

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("group.id", this.groupId);
        // Don't re-consume old messages from input by default
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("metadata.broker.list", inputBroker);
        // Newer version of metadata.broker.list:
        kafkaParams.put("bootstrap.servers", inputBroker);
        kafkaParams.put("fetch.message.max.bytes", "10485760");

        Map<TopicAndPartition, Long> offsets =
                cn.migu.streaming.kafka.KafkaUtils.getOffsets(inputTopicLockMaster, this.groupId, inputTopic);
        fillInLatestOffsets(offsets, kafkaParams);
        log.info("Initial offsets: {}", offsets);

        /*DataTraceModel traceLog = DataTraceLog.getInstance().makeDataTraceModel(dataCache, false);
        DataTraceLog.getInstance().initTrace(traceLog, offsets);*/

        @SuppressWarnings("unchecked") Class<MessageAndMetadata<K, M>> streamClass =
                (Class<MessageAndMetadata<K, M>>) (Class<?>) MessageAndMetadata.class;

        return KafkaUtils.createDirectStream(streamingContext,
                keyClass,
                messageClass,
                keyDecoderClass,
                messageDecoderClass,
                streamClass,
                kafkaParams,
                offsets,
                Functions.<MessageAndMetadata<K, M>>identity());
    }

    /**
     * 转换(topic-key,topic-message)的元组
     *
     * @param <K> topic键值
     * @param <M> topci中的消息
     */
    public static final class MMDToTuple2Fn<K, M> implements PairFunction<MessageAndMetadata<K, M>, K, M> {
        @Override
        public Tuple2<K, M> call(MessageAndMetadata<K, M> km) {
            return new Tuple2<>(km.key(), km.message());
        }
    }

    /**
     * 获取最近的可用的kafka offset
     *
     * @param offsets     topic-partition-offset映射
     * @param kafkaParams kafka配置参数
     */
    private void fillInLatestOffsets(Map<TopicAndPartition, Long> offsets, Map<String, String> kafkaParams) {
        @SuppressWarnings("unchecked") scala.collection.immutable.Map<String, String> kafkaParamsScalaMap =
                (scala.collection.immutable.Map<String, String>) scala.collection.immutable.Map$.MODULE$.apply(
                        JavaConversions.mapAsScalaMap(kafkaParams).toSeq());

        KafkaCluster kc = new KafkaCluster(kafkaParamsScalaMap);

        if (offsets.containsValue(null)) {

            Set<TopicAndPartition> needOffset = new HashSet<>();
            for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
                if (entry.getValue() == null) {
                    needOffset.add(entry.getKey());
                }
            }
            log.info("No initial offsets for {}; reading from Kafka", needOffset);

            @SuppressWarnings("unchecked") scala.collection.immutable.Set<TopicAndPartition> needOffsetScalaSet =
                    (scala.collection.immutable.Set<TopicAndPartition>) scala.collection.immutable.Set$.MODULE$.apply(
                            JavaConversions.asScalaSet(needOffset).toSeq());

            Map<TopicAndPartition, ?> leaderOffsets =
                    JavaConversions.mapAsJavaMap(kc.getLatestLeaderOffsets(needOffsetScalaSet).right().get());
            for (Map.Entry<TopicAndPartition, ?> entry : leaderOffsets.entrySet()) {
                TopicAndPartition tAndP = entry.getKey();
                // Can't reference LeaderOffset class, so, hack away:
                String leaderOffsetString = entry.getValue().toString();
                Matcher m = Pattern.compile("LeaderOffset\\([^,]+,[^,]+,([^)]+)\\)").matcher(leaderOffsetString);
                Preconditions.checkState(m.matches());
                offsets.put(tAndP, Long.valueOf(m.group(1)));
            }
        }

        //校验offset在有效范围之内
        Set<TopicAndPartition> allOffsets =
                offsets.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());

        @SuppressWarnings("unchecked") scala.collection.immutable.Set<TopicAndPartition> allOffsetScalaSet =
                (scala.collection.immutable.Set<TopicAndPartition>) scala.collection.immutable.Set$.MODULE$.apply(
                        JavaConversions.asScalaSet(allOffsets).toSeq());
        Map<TopicAndPartition, ?> lastestOffsets =
                JavaConversions.mapAsJavaMap(kc.getLatestLeaderOffsets(allOffsetScalaSet).right().get());

        Map<TopicAndPartition, ?> earliestOffsets =
                JavaConversions.mapAsJavaMap(kc.getEarliestLeaderOffsets(allOffsetScalaSet).right().get());

        for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
            TopicAndPartition tAndP = entry.getKey();
            long bOffset = entry.getValue();
            String earliestOffsetStr = earliestOffsets.get(entry.getKey()).toString();

            long earliestOffset = getOffsetLongType(earliestOffsetStr);

            String lastestOffsetStr = lastestOffsets.get(entry.getKey()).toString();
            long lastestOffset = getOffsetLongType(lastestOffsetStr);

            if (bOffset < earliestOffset || bOffset > lastestOffset) {
                offsets.put(tAndP, earliestOffset);
            }
        }

    }

    /**
     * 获取offset值
     *
     * @param offsetStr offset字符串
     */
    private long getOffsetLongType(String offsetStr) {
        Matcher m = Pattern.compile("LeaderOffset\\([^,]+,[^,]+,([^)]+)\\)").matcher(offsetStr);
        Preconditions.checkState(m.matches());
        return Long.valueOf(m.group(1));
    }

}
