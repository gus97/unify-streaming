

package cn.migu.streaming.kafka;

import cn.migu.streaming.db.BatchMonitor;
import cn.migu.streaming.db.DataTraceLog;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.DataTraceModel;
import cn.migu.streaming.model.FileCleanConf;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 在streaming job中读取RDD中的offset,并在业务处理完成后更新offset
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public final class UpdateOffsetsFn<K, M> implements VoidFunction<JavaRDD<MessageAndMetadata<K, M>>> {

    private static final Logger log = LoggerFactory.getLogger("streaming");

    private final String group;

    private final String inputTopicLockMaster;

    private final DataCache dataCache;

    public UpdateOffsetsFn(String group, String inputTopicLockMaster, DataCache dataCache) {
        this.group = group;
        this.inputTopicLockMaster = inputTopicLockMaster;
        this.dataCache = dataCache;
    }

    /**
     * @param javaRDD javaRDD实例
     * @return null
     */
    @Override
    public void call(JavaRDD<MessageAndMetadata<K, M>> javaRDD) {
        OffsetRange[] ranges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
        Map<TopicAndPartition, Long> newOffsets = new HashMap<>(ranges.length);
        for (OffsetRange range : ranges) {
            newOffsets.put(new TopicAndPartition(range.topic(), range.partition()), range.untilOffset());
        }
        log.info("Updating offsets: {}", newOffsets);

        BatchMonitor batchMonitor = BatchMonitor.getInstance();

        FileCleanConf fcc = this.dataCache.getFileCleanConf();
        batchMonitor.update(fcc.getMsgCountfromKafka(), fcc.getMsgCountAfterClean(), this.dataCache.getJarConfParam().getJarId());

        DataTraceModel traceLog = DataTraceLog.getInstance().makeDataTraceModel(this.dataCache, true);
        DataTraceLog.getInstance().updateTrace(traceLog, newOffsets);

        KafkaUtils.setOffsets(inputTopicLockMaster, group, newOffsets);
    }

}
