package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * 预加载输入数据表实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class PreloadTb implements Serializable
{
    private String tableName;

    private String runSql;

    private String dataSourceId;

    private String refreshTime;

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getRunSql()
    {
        return runSql;
    }

    public void setRunSql(String runSql)
    {
        this.runSql = runSql;
    }

    public String getDataSourceId()
    {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId)
    {
        this.dataSourceId = dataSourceId;
    }

    public String getRefreshTime()
    {
        return refreshTime;
    }

    public void setRefreshTime(String refreshTime)
    {
        this.refreshTime = refreshTime;
    }

    @Override public String toString()
    {
        return "PreloadTb{" + "tableName='" + tableName + '\'' + ", runSql='" + runSql + '\'' + ", dataSourceId='"
            + dataSourceId + '\'' + ", refreshTime='" + refreshTime + '\'' + '}';
    }
}
