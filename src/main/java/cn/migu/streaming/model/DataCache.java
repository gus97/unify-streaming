package cn.migu.streaming.model;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * streaming job业务处理数据上下文
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class DataCache implements Serializable
{
    private JarConfParam jarConfParam;

    private final Map<String, DataType> colMapType;

    private KafkaConfigParam kafkaConf;

    private FileCleanConf fileCleanConf;

    private List<ColumnCleanConf> colsCleanConf;

    private List<PreloadTb> preloadTbs;

    private List<OperateSql> outputSqls;

    private SparkSession sparkSession;

    //private transient Connection connection;

    private transient Statement statement;

    public DataCache(KafkaConfigParam kafkaConf, FileCleanConf fileCleanConf, List<ColumnCleanConf> colsCleanConf,
        List<OperateSql> outputSqls)
    {
        this.kafkaConf = kafkaConf;
        this.fileCleanConf = fileCleanConf;
        this.colsCleanConf = colsCleanConf;
        this.outputSqls = outputSqls;

        colMapType = new HashMap<>();
        colMapType.put("string", DataTypes.StringType);
        colMapType.put("int", DataTypes.IntegerType);
        colMapType.put("float", DataTypes.FloatType);
        colMapType.put("double", DataTypes.DoubleType);
        colMapType.put("date", DataTypes.DateType);
        colMapType.put("timestamp", DataTypes.TimestampType);
    }

    public JarConfParam getJarConfParam()
    {
        return jarConfParam;
    }

    public void setJarConfParam(JarConfParam jarConfParam)
    {
        this.jarConfParam = jarConfParam;
    }

    public KafkaConfigParam getKafkaConf()
    {
        return kafkaConf;
    }

    public void setKafkaConf(KafkaConfigParam kafkaConf)
    {
        this.kafkaConf = kafkaConf;
    }

    public FileCleanConf getFileCleanConf()
    {
        return fileCleanConf;
    }

    public void setFileCleanConf(FileCleanConf fileCleanConf)
    {
        this.fileCleanConf = fileCleanConf;
    }

    public List<ColumnCleanConf> getColsCleanConf()
    {
        return colsCleanConf;
    }

    public void setColsCleanConf(List<ColumnCleanConf> colsCleanConf)
    {
        this.colsCleanConf = colsCleanConf;
    }

    public List<PreloadTb> getPreloadTbs()
    {
        return preloadTbs;
    }

    public void setPreloadTbs(List<PreloadTb> preloadTbs)
    {
        this.preloadTbs = preloadTbs;
    }

    public List<OperateSql> getOutputSqls()
    {
        return outputSqls;
    }

    public void setOutputSqls(List<OperateSql> outputSqls)
    {
        this.outputSqls = outputSqls;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Map<String, DataType> getColMapType()
    {
        return colMapType;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public void setStatement(Statement statement)
    {
        this.statement = statement;
    }
}
