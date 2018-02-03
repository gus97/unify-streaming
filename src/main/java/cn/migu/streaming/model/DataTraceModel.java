package cn.migu.streaming.model;

import java.io.Serializable;
import java.util.Date;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2016/10/14]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class DataTraceModel
{
    private String appId;

    private String serverId;

    private String jarId;

    private String topic;

    private String groupid;

    private int partition;

    private long offset;

    private int revCount;

    private int hdlCount;

    private long totalCount;

    private String processid;

    public String getAppId()
    {
        return appId;
    }

    public void setAppId(String appId)
    {
        this.appId = appId;
    }

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

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public String getGroupid()
    {
        return groupid;
    }

    public void setGroupid(String groupid)
    {
        this.groupid = groupid;
    }

    public int getPartition()
    {
        return partition;
    }

    public void setPartition(int partition)
    {
        this.partition = partition;
    }

    public long getOffset()
    {
        return offset;
    }

    public void setOffset(long offset)
    {
        this.offset = offset;
    }

    public int getRevCount()
    {
        return revCount;
    }

    public void setRevCount(int revCount)
    {
        this.revCount = revCount;
    }

    public int getHdlCount()
    {
        return hdlCount;
    }

    public void setHdlCount(int hdlCount)
    {
        this.hdlCount = hdlCount;
    }

    public long getTotalCount()
    {
        return totalCount;
    }

    public void setTotalCount(long totalCount)
    {
        this.totalCount = totalCount;
    }

    public String getProcessid()
    {
        return processid;
    }

    public void setProcessid(String processid)
    {
        this.processid = processid;
    }

}
