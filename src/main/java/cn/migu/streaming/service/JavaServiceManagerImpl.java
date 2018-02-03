package cn.migu.streaming.service;

import cn.migu.streaming.etl.Content;
import cn.migu.streaming.etl.Filter;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.spark.AbstractSparkLayer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * java业务处理实现类(清洗数据被搜集至driver端统一处理)
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class JavaServiceManagerImpl extends AbstractServiceManager<String, String, String>
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    public JavaServiceManagerImpl(DataCache cache)
    {
        super(cache);
    }

    /**
     * 业务处理流程
     * @param newData 数据rdd实例
     * @return null
     */
    @Override protected JavaRDD<String> handler(JavaPairRDD<String, String> newData)
        throws IOException
    {
        List<String> lines = this.collectLines(newData);

        log.info("文件清洗前总行数:{}", lines.size());

        this.getCache().getFileCleanConf().setMsgCountfromKafka(lines.size());

        List<String> newContents = lines.stream()
            .filter(line -> Filter.filter(line, getCache()))
            //.map(line -> Content.getContent(line))
            .collect(Collectors.toList());

        log.info("文件清洗后总行数:{}", newContents.size());

        this.getCache().getFileCleanConf().setMsgCountAfterClean(newContents.size());

        //JavaSparkContext.fromSparkContext(this.cache.getHiveContext().sparkContext());

        return AbstractSparkLayer.SJSC.parallelize(newContents);
    }

    /**
     * 数据搜集至driver,并返回List
     * @param newData 数据rdd实例
     * @return null
     */
    private List<String> collectLines(JavaPairRDD<String, String> newData)
    {

        //return newData.values().flatMap(s -> Collections.singletonList(s)).collect();
        return newData.values().collect();
        /*return newData.values().flatMap(new FlatMapFunction<String, String>()
        {
            @Override public Iterable<String> call(String s)
                throws Exception
            {
                List<String> rst = new ArrayList<String>();
                rst.add(s);
                return rst;
            }
        }).collect();*/
    }
}
