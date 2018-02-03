package cn.migu.streaming.service;

import cn.migu.streaming.etl.Content;
import cn.migu.streaming.etl.Filter;
import cn.migu.streaming.model.DataCache;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * 业务处理实现类(清洗数据被推送至worker端处理)
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class ServiceManagerImpl extends AbstractServiceManager<String, String, String>
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    public ServiceManagerImpl(DataCache cache)
    {
        super(cache);
    }

    /**
     * 业务处理流程
     * @param newData 数据rdd实例
     * @return null
     */
    @Override public JavaRDD<String> handler(JavaPairRDD<String, String> newData)
        throws IOException
    {

        int bcount = (int)newData.count();
        JavaPairRDD<String, String> newRepartitionData = newData.repartition(Integer.valueOf(this.getCache().getJarConfParam().getTotalCores()));
        log.info("分区大小:{}", newRepartitionData.rdd().partitions().length);
        log.info("文件清洗前总行数:{}", bcount);

        this.getCache().getFileCleanConf().setMsgCountfromKafka(bcount);

        JavaRDD<String> rightData = newRepartitionData.values()
            .filter(s -> Filter.filter(s, getCache()))
            //.map(s -> Content.getContent(s))
            .flatMap(s -> Collections.singletonList(s).iterator());
        //log.info("分区大小:{}", rightData.rdd().partitions().length);

        /*JavaRDD<String> rightData = newData.values().filter(new Function<String, Boolean>()
        {
            @Override public Boolean call(String s)
                throws Exception
            {

                return Filter.filter(s, getCache());
            }
        }).flatMap(new FlatMapFunction<String, String>()
        {

            @Override public Iterable<String> call(String s)
                throws Exception
            {
                String line = Content.getContent(s);

                List<String> rst = new ArrayList<String>();
                rst.add(line);
                return rst;
            }
        });*/
        int acount = (int)rightData.count();

        log.info("文件清洗后总行数:{}", acount);

        this.getCache().getFileCleanConf().setMsgCountAfterClean(acount);

        return rightData;


    }

}
