package cn.migu.streaming.service;

import cn.migu.streaming.etl.Content;
import cn.migu.streaming.model.ColumnCleanConf;
import cn.migu.streaming.model.DataCache;
import cn.migu.streaming.model.FileCleanConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * streaming业务处理抽象基类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public abstract class AbstractServiceManager<K, M, U> implements Serializable
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    private DataCache cache;

    public AbstractServiceManager(DataCache cache)
    {
        this.cache = cache;
    }

    protected abstract JavaRDD<U> handler(JavaPairRDD<K, M> newData)
        throws IOException;

    /**
     * 加载处理后数据至内存表
     * @param newData 数据rdd实例
     * @return null
     */
    protected boolean loadDf(JavaRDD<String> newData)
    {
        if (!newData.isEmpty())
        {
            JavaRDD<Row> tData = this.formatData(newData);

            StructType schema = this.createSchema();
            /*Row firstRow = tData.first();
            log.info("第一列内容{}", firstRow.toString());
            log.info("数据列数:{},schema列数为{}", firstRow.size(), schema.length());*/
            //增量数据表
            Dataset<Row> deltaTb = this.cache.getSparkSession().createDataFrame(tData, schema);

            String tableName = this.cache.getFileCleanConf().getTableName();

            deltaTb.registerTempTable(StringUtils.join(tableName, "_bak"));

            FileCleanConf fcc = this.cache.getFileCleanConf();

            JavaRDD<Row> totalData = tData;

            if (fcc.isSaveAllData())
            {
                JavaRDD<Row> btd = fcc.getFullData();

                if (null != btd)
                {
                    totalData = btd.union(totalData);
                    //totalData = AbstractSparkLayer.SJSC.union(btd, totalData);

                    //totalData.checkpoint();

                }

                fcc.setFullData(totalData);
            }

            Dataset<Row> allTb = this.cache.getSparkSession().createDataFrame(totalData, schema);


            log.info("注册内存表为:{}", tableName);

            allTb.registerTempTable(tableName);

            //allTb.show();

            return true;

        }
        else
        {
            log.info("清洗后无数据加载到dataframe");

            return false;
        }
    }

    /**
     * 格式化数据Row
     * @param newData 数据rdd实例
     * @return null
     */
    private JavaRDD<Row> formatData(JavaRDD<String> newData)
    {
        return newData.map(new Function<String, Row>()
        {
            List<ColumnCleanConf> colsConf = AbstractServiceManager.this.cache.getColsCleanConf();

            int rColNum = colsConf.size();

            int cColNum = AbstractServiceManager.this.cache.getFileCleanConf().getColNum();

            int uColNum = rColNum > (cColNum + 1) ? (cColNum + 1) : rColNum;

            //Object[] objArr = new Object[uColNum];

            List<Object> objList = new ArrayList<Object>();

            @Override public Row call(String s)
                throws Exception
            {
                objList.clear();

                String metadata = Content.getFileMetaData(s);

                String content = Content.getContent(s);

                String[] cols = Content.splitToColumns(content, AbstractServiceManager.this.cache);

                Object attr = null;

                //第一列配置为文件metadata信息
                if (!colsConf.get(0).isDelete())
                {
                    objList.add(metadata);
                }

                for (int i = 1; i < uColNum; i++)
                {
                    ColumnCleanConf colConf = colsConf.get(i);
                    if (colConf.isDelete())
                    {
                        continue;
                    }

                    String value = cols[i - 1];
                    String kind = colConf.getKind();

                    switch (kind)
                    {
                        case "string":
                            attr = value;
                            break;
                        case "int":
                            attr = Integer.valueOf(value.trim());
                            break;
                        case "float":
                            attr = Float.valueOf(value.trim());
                            break;
                        case "double":
                            attr = Double.valueOf(value.trim());
                            break;
                        case "date":
                        case "timestamp":
                            attr = value.trim();
                            break;
                    }
                    //objArr[i] = attr;
                    objList.add(attr);
                }
                return RowFactory.create(objList.toArray());
            }
        });
    }

    /**
     * 创建内存表schema
     * @return StructType
     */
    private StructType createSchema()
    {
        List<StructField> structFields = new ArrayList<StructField>();
        List<ColumnCleanConf> colCleanConf = this.cache.getColsCleanConf();

        for (ColumnCleanConf ccc : colCleanConf)
        {
            if (ccc.isDelete())
            {
                continue;
            }
            String kind = ccc.getKind();
            structFields.add(DataTypes.createStructField(ccc.getField(), this.cache.getColMapType().get(kind), true));

        }

        return DataTypes.createStructType(structFields);
    }

    public DataCache getCache()
    {
        return cache;
    }

    public void setCache(DataCache cache)
    {
        this.cache = cache;
    }
}
