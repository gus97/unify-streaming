package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * 业务输出sql实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class OperateSql implements Serializable
{
    private String sql;

    private String mapTable;

    private String sourceId;

    private int runMode;

    private int seq;

    public String getSql()
    {
        return sql;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    public String getMapTable()
    {
        return mapTable;
    }

    public void setMapTable(String mapTable)
    {
        this.mapTable = mapTable;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public void setSourceId(String sourceId)
    {
        this.sourceId = sourceId;
    }

    public int getRunMode()
    {
        return runMode;
    }

    public void setRunMode(int runMode)
    {
        this.runMode = runMode;
    }

    public int getSeq()
    {
        return seq;
    }

    public void setSeq(int seq)
    {
        this.seq = seq;
    }

    @Override public String toString()
    {
        return "OperateSql{" + "sql='" + sql + '\'' + ", mapTable='" + mapTable + '\'' + ", sourceId='" + sourceId
            + '\'' + ", runMode=" + runMode + ", seq=" + seq + '}';
    }
}
