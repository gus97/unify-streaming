
package cn.migu.streaming.spark;

import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import cn.migu.unify.hugetable.jdbc.DbUtils;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import cn.migu.streaming.db.RegisterProcess;
import cn.migu.streaming.etl.Input;
import cn.migu.streaming.kafka.UpdateOffsetsFn;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DateFormatStr;
import cn.migu.streaming.model.PreloadTb;
import cn.migu.streaming.service.ServiceFunction;
import kafka.message.MessageAndMetadata;

/**
 * spark streaming计算实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public final class StreamingComputeLayer<K, M, U> extends AbstractSparkLayer<K, M> {

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private String layerName;

    private final DataCache dataCache;

    private JavaStreamingContext streamingContext;

    public StreamingComputeLayer(Config config, DataCache cache) {
        super(config, cache);

        this.dataCache = cache;
    }

    @Override
    protected String getLayerName() {
        return this.layerName;
    }


    public synchronized void start() {

        this.layerName = StringUtils.join("streaming_",
                this.dataCache.getJarConfParam().getAppId(),
                "_",
                this.dataCache.getJarConfParam().getAppName());

        streamingContext = buildStreamingContext(this.dataCache);

        /*int delayMultiple = this.getConfig().getInt("delay-criterion-multiple");

        long delayThreshold = generationIntervalSec * delayMultiple * 1000;

        //kill spark app restful地址
        String killAppUrl = StringUtils.join("http://",
            StringUtils.split(StringUtils.substringAfter(this.dataCache.getJarConfParam().getSparkMaster(), "spark://"),
                ":")[0],
            ":",
            this.getConfig().getString("master-webui-port"),
            "/app/kill/");

        streamingContext.addStreamingListener(new StreamingCustomListener(streamingContext,
            delayThreshold,
            killAppUrl));*/

        //预加载数据
        Input preData = Input.getInstance();
        List<PreloadTb> ptc = preData.preloadTbConf(getConfig(), this.dataCache);
        this.dataCache.setPreloadTbs(ptc);
        preData.load(getConfig(), this.dataCache);

        /*String ckDirString = this.getConfig().getString("hdfs-ckdir");
        Path checkpointPath = new Path(new Path(ckDirString), this.getLayerName());
        log.info("Setting checkpoint dir to {}", checkpointPath);
        streamingContext.sparkContext().setCheckpointDir(checkpointPath.toString());*/

        log.info("Creating message stream from topic");
        JavaInputDStream<MessageAndMetadata<K, M>> dStream = buildInputDStream(streamingContext, this.dataCache);

        //进程注册
        RegisterProcess rp = new RegisterProcess();
        rp.register(this.dataCache.getJarConfParam(), streamingContext.sparkContext().sc());

        //初始化时间
        DateFormatStr.instance().init();

        JavaPairDStream<K, M> pairDStream = dStream.mapToPair(new MMDToTuple2Fn<K, M>());

        pairDStream.foreachRDD(new ServiceFunction<>(getConfig(), this.dataCache));

        //Class<K> keyClass = getKeyClass();
        //Class<M> messageClass = getMessageClass();
        /*pairDStream.foreachRDD(new BatchUpdateFunction<>(getConfig(),
            keyClass,
            messageClass,
            keyWritableClass,
            messageWritableClass,
            dataDirString,
            modelDirString,
            loadUpdateInstance(),
            streamingContext));*/

        // "Inline" saveAsNewAPIHadoopFiles to be able to skip saving empty RDDs
        /*pairDStream.foreachRDD(new SaveToHDFSFunction<>(dataDirString + "/oryx",
            "data",
            keyClass,
            messageClass,
            keyWritableClass,
            messageWritableClass,
            streamingContext.sparkContext().hadoopConfiguration()));*/

        dStream.foreachRDD(new UpdateOffsetsFn<K, M>(this.getGroupID(), getInputTopicLockMaster(), this.dataCache));

        /*if (maxDataAgeHours != NO_MAX_DATA_AGE)
        {inputTopicLockMaster
            dStream.foreachRDD(new DeleteOldDataFn<MessageAndMetadata<K, M>>(streamingContext.sparkContext()
                .hadoopConfiguration(), dataDirString, maxDataAgeHours));
        }*/

        this.bootSuccessAck();
        log.info("-------------------Starting Spark Streaming------------------------");
        streamingContext.start();
    }

    public void await() {
        Preconditions.checkState(streamingContext != null);

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void close() {
        if (null != dataCache) {
            Statement stat = dataCache.getStatement();
            if (null != stat) {
                log.info("cancel hugetable statement");
                //DbUtils.cancel(stat);
                log.info("cancel hugetable statement end");
            }
        }

        /*if (streamingContext != null)
        {
            log.info("Shutting down Spark Streaming; this may take some time");
            streamingContext.stop(true, true);
            streamingContext = null;
            log.info("Shutting down Spark Streaming ending...");
        }*/

        System.exit(1);
    }

    /**
     * 启动成功标识(ugly)
     */
    private void bootSuccessAck() {
        System.err.println("STREAING STARTED");
    }

}
