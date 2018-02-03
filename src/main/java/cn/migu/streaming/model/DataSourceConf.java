package cn.migu.streaming.model;

import java.io.Serializable;

/**
 * 数据源配置实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class DataSourceConf implements Serializable
{
    private int kind;

    private String tableName;

    private String connectUrl;

    private String driver;

    private String userName;

    private String password;

    private String decollator = "";

    private String address;

    public int getKind()
    {
        return kind;
    }

    public void setKind(int kind)
    {
        this.kind = kind;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getConnectUrl()
    {
        return connectUrl;
    }

    public void setConnectUrl(String connectUrl)
    {
        this.connectUrl = connectUrl;
    }

    public String getDriver()
    {
        return driver;
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = (null == password) ? "" : password;
    }

    public String getDecollator()
    {
        return decollator;
    }

    public void setDecollator(String decollator)
    {
        if (null != decollator)
        {
            this.decollator = decollator;
        }

    }

    public String getAddress()
    {
        return address;
    }

    public void setAddress(String address)
    {
        this.address = address;
    }

    @Override public String toString()
    {
        return "DataSourceConf{" + "kind=" + kind + ", tableName='" + tableName + '\'' + ", connectUrl='" + connectUrl
            + '\'' + ", driver='" + driver + '\'' + ", userName='" + userName + '\'' + ", password='" + password + '\''
            + ", decollator='" + decollator + '\'' + ", address='" + address + '\'' + '}';
    }
}
