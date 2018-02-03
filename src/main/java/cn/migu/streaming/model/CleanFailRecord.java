package cn.migu.streaming.model;

import java.io.Serializable;
import java.util.Date;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2017/2/6]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class CleanFailRecord implements Serializable
{
    private String serverId;

    private String jarId;

    private String sourceId;

    private int failKind;

    private String failRecord;

    private String failCode;

    private int startNum;

    private Date dealTime;

    public String getServerId()
    {
        return serverId;
    }

    public void setServerId(String serverId)
    {
        this.serverId = serverId;
    }

    public String getJarId()
    {
        return jarId;
    }

    public void setJarId(String jarId)
    {
        this.jarId = jarId;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public void setSourceId(String sourceId)
    {
        this.sourceId = sourceId;
    }

    public int getFailKind()
    {
        return failKind;
    }

    public void setFailKind(int failKind)
    {
        this.failKind = failKind;
    }

    public String getFailRecord()
    {
        return failRecord;
    }

    public void setFailRecord(String failRecord)
    {
        this.failRecord = failRecord;
    }

    public String getFailCode()
    {
        return failCode;
    }

    public void setFailCode(String failCode)
    {
        this.failCode = failCode;
    }

    public int getStartNum()
    {
        return startNum;
    }

    public void setStartNum(int startNum)
    {
        this.startNum = startNum;
    }

    public Date getDealTime()
    {
        return dealTime;
    }

    public void setDealTime(Date dealTime)
    {
        this.dealTime = dealTime;
    }
}
