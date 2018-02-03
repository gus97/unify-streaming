package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * 文件清洗配置实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class JarCleanConf implements Serializable
{
    private String isclean;

    private String issavealldata;

    private String runsql;

    public String getIsclean()
    {
        return isclean;
    }

    public void setIsclean(String isclean)
    {
        this.isclean = isclean;
    }

    public String getIssavealldata()
    {
        return issavealldata;
    }

    public void setIssavealldata(String issavealldata)
    {
        this.issavealldata = issavealldata;
    }

    public String getRunsql()
    {
        return runsql;
    }

    public void setRunsql(String runsql)
    {
        this.runsql = runsql;
    }
}
