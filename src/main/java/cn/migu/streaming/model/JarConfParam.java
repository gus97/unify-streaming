package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * jar运行输入参数实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class JarConfParam implements Serializable
{
    private String jarId;

    private String serviceId;

    private String appId;

    private String sparkMaster;

    private String hdfsPath;

    private String jarPort;

    private String appName;

    private String dealUser;

    private String totalCores;

    private String executorMemory;

    private String maxRatePerPartition;

    private String duration;

    public JarConfParam()
    {

    }

    public JarConfParam(String jarId, String serviceId, String appId)
    {
        this.jarId = jarId;
        this.serviceId = serviceId;
        this.appId = appId;
    }

    public String getJarId()
    {
        return jarId;
    }

    public void setJarId(String jarId)
    {
        this.jarId = jarId;
    }

    public String getServiceId()
    {
        return serviceId;
    }

    public void setServiceId(String serviceId)
    {
        this.serviceId = serviceId;
    }

    public String getAppId()
    {
        return appId;
    }

    public void setAppId(String appId)
    {
        this.appId = appId;
    }

    public String getSparkMaster()
    {
        return sparkMaster;
    }

    public void setSparkMaster(String sparkMaster)
    {
        this.sparkMaster = sparkMaster;
    }

    public String getHdfsPath()
    {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath)
    {
        this.hdfsPath = hdfsPath;
    }

    public String getJarPort()
    {
        return jarPort;
    }

    public void setJarPort(String jarPort)
    {
        this.jarPort = jarPort;
    }

    public String getAppName()
    {
        return appName;
    }

    public void setAppName(String appName)
    {
        this.appName = appName;
    }

    public String getDealUser()
    {
        return dealUser;
    }

    public void setDealUser(String dealUser)
    {
        this.dealUser = dealUser;
    }

    public String getTotalCores()
    {
        return totalCores;
    }

    public void setTotalCores(String totalCores)
    {
        this.totalCores = totalCores;
    }

    public String getExecutorMemory()
    {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory)
    {
        this.executorMemory = executorMemory;
    }

    public String getMaxRatePerPartition()
    {
        return maxRatePerPartition;
    }

    public void setMaxRatePerPartition(String maxRatePerPartition)
    {
        this.maxRatePerPartition = maxRatePerPartition;
    }

    public String getDuration()
    {
        return duration;
    }

    public void setDuration(String duration)
    {
        this.duration = duration;
    }

    @Override public String toString()
    {
        return "JarConfParam{" + "jarId='" + jarId + '\'' + ", serviceId='" + serviceId + '\'' + ", appId='" + appId
            + '\'' + ", sparkMaster='" + sparkMaster + '\'' + ", hdfsPath='" + hdfsPath + '\'' + ", jarPort=" + jarPort
            + ", appName='" + appName + '\'' + ", dealUser='" + dealUser + '\'' + ", totalCores=" + totalCores
            + ", executorMemory='" + executorMemory + '\'' + ", maxRatePerPartition=" + maxRatePerPartition
            + ", duration=" + duration + '}';
    }
}
