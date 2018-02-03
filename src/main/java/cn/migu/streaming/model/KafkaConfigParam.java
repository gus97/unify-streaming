package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * kafka配置实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class KafkaConfigParam implements Serializable
{
    private String name;

    private int kind;

    private String zkconnect;

    private String brokers;

    private String username;

    private String password;

    private String topic;

    private String objId;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getKind()
    {
        return kind;
    }

    public void setKind(int kind)
    {
        this.kind = kind;
    }

    public String getZkconnect()
    {
        return zkconnect;
    }

    public void setZkconnect(String zkconnect)
    {
        this.zkconnect = zkconnect;
    }

    public String getBrokers()
    {
        return brokers;
    }

    public void setBrokers(String brokers)
    {
        this.brokers = brokers;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public String getObjId()
    {
        return objId;
    }

    public void setObjId(String objId)
    {
        this.objId = objId;
    }

    @Override public String toString()
    {
        return "KafkaConfigParam{" + "name='" + name + '\'' + ", kind=" + kind + ", zkconnect='" + zkconnect + '\''
            + ", brokers='" + brokers + '\'' + ", username='" + username + '\'' + ", password='" + password + '\''
            + ", topic='" + topic + '\'' + ", objId='" + objId + '\'' + '}';
    }
}
