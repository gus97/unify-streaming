/*
 * 文 件 名:  SQLLogModel.java
 * 版    权:  Copyright 2016 咪咕互动娱乐有限公司,  All rights reserved
 * 描    述:  <描述>
 * 版    本： <版本号> 
 * 创 建 人:  lizhi
 * 创建时间:  2016年6月6日
 
 */
package cn.migu.streaming.model;

import java.io.Serializable;
import java.util.Date;

/**
 * SQL运行日志MODEL
 * <功能详细描述>
 *
 * @author zhaocan
 * @version [版本号, 2016年9月12日]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class SqlLogModel implements Serializable
{
    //所属系统
    private String appId;

    //所属服务器
    private String serverId;

    //所属jar
    private String jarId;

    //类别
    private int kind;

    //顺序
    private int seq;

    //SQL
    private String jarSql;

    //开始时间
    private Date startTime;

    //结束时间
    private Date endTime;

    //运行状态
    private int state;

    //任务耗时
    private long elapse;

    //运行说明
    private String note;

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

    public int getKind()
    {
        return kind;
    }

    public void setKind(int kind)
    {
        this.kind = kind;
    }

    public int getSeq()
    {
        return seq;
    }

    public void setSeq(int seq)
    {
        this.seq = seq;
    }

    public String getJarSql()
    {
        return jarSql;
    }

    public void setJarSql(String jarSql)
    {
        this.jarSql = jarSql;
    }

    public Date getStartTime()
    {
        return startTime;
    }

    public void setStartTime(Date startTime)
    {
        this.startTime = startTime;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public void setEndTime(Date endTime)
    {
        this.endTime = endTime;
    }

    public int getState()
    {
        return state;
    }

    public void setState(int state)
    {
        this.state = state;
    }

    public long getElapse()
    {
        return elapse;
    }

    public void setElapse(long elapse)
    {
        this.elapse = elapse;
    }

    public String getNote()
    {
        return note;
    }

    public void setNote(String note)
    {
        this.note = note;
    }
}
