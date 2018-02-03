package cn.migu.streaming.model;

import cn.migu.streaming.common.DateUtils;

import java.io.Serializable;

/**
 * 日期格式字符串实体类
 *
 * @author zhaocan
 * @version [版本号, 2016-09-12]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public class DateFormatStr implements Serializable
{
    private String inLineToDay;

    private String inLineToHour;

    private String inLineToMinite;

    private String inLineToSec;

    private String compactToDay;

    private String compactToHour;

    private String compactToMinite;

    private String compactToSec;

    //
    private String inLineToDayFixJar;

    private String inLineToHourFixJar;

    private String inLineToMiniteFixJar;

    private String inLineToSecFixJar;

    private String compactToDayFixJar;

    private String compactToHourFixJar;

    private String compactToMiniteFixJar;

    private String compactToSecFixJar;




    private static DateFormatStr dateFormatStr;

    private DateFormatStr()
    {

    }

    public static DateFormatStr instance()
    {
        if (null == dateFormatStr)
        {
            dateFormatStr = new DateFormatStr();
        }

        return dateFormatStr;
    }

    public void update()
    {
        this.inLineToDay = DateUtils.getSpecFormatDate("yyyy-MM-dd");
        this.inLineToHour = DateUtils.getSpecFormatDate("yyyy-MM-dd HH");
        this.inLineToMinite = DateUtils.getSpecFormatDate("yyyy-MM-dd HH:mm");
        this.inLineToSec = DateUtils.getSpecFormatDate("yyyy-MM-dd HH:mm:ss");

        this.compactToDay = DateUtils.getSpecFormatDate("yyyyMMdd");
        this.compactToHour = DateUtils.getSpecFormatDate("yyyyMMdd HH");
        this.compactToMinite = DateUtils.getSpecFormatDate("yyyyMMdd HH:mm");
        this.compactToSec = DateUtils.getSpecFormatDate("yyyyMMdd HH:mm:ss");
    }

    public void init()
    {
        this.inLineToDayFixJar = DateUtils.getSpecFormatDate("yyyy-MM-dd");
        this.inLineToHourFixJar = DateUtils.getSpecFormatDate("yyyy-MM-dd HH");
        this.inLineToMiniteFixJar = DateUtils.getSpecFormatDate("yyyy-MM-dd HH:mm");
        this.inLineToSecFixJar = DateUtils.getSpecFormatDate("yyyy-MM-dd HH:mm:ss");

        this.compactToDayFixJar = DateUtils.getSpecFormatDate("yyyyMMdd");
        this.compactToHourFixJar = DateUtils.getSpecFormatDate("yyyyMMdd HH");
        this.compactToMiniteFixJar = DateUtils.getSpecFormatDate("yyyyMMdd HH:mm");
        this.compactToSecFixJar = DateUtils.getSpecFormatDate("yyyyMMdd HH:mm:ss");
    }

    public String getInLineToDay()
    {
        return inLineToDay;
    }

    public String getInLineToHour()
    {
        return inLineToHour;
    }

    public String getInLineToMinite()
    {
        return inLineToMinite;
    }

    public String getInLineToSec()
    {
        return inLineToSec;
    }

    public String getCompactToDay()
    {
        return compactToDay;
    }

    public String getCompactToHour()
    {
        return compactToHour;
    }

    public String getCompactToMinite()
    {
        return compactToMinite;
    }

    public String getCompactToSec()
    {
        return compactToSec;
    }

    public String getInLineToDayFixJar()
    {
        return inLineToDayFixJar;
    }

    public String getInLineToHourFixJar()
    {
        return inLineToHourFixJar;
    }

    public String getInLineToMiniteFixJar()
    {
        return inLineToMiniteFixJar;
    }

    public String getInLineToSecFixJar()
    {
        return inLineToSecFixJar;
    }

    public String getCompactToDayFixJar()
    {
        return compactToDayFixJar;
    }

    public String getCompactToHourFixJar()
    {
        return compactToHourFixJar;
    }

    public String getCompactToMiniteFixJar()
    {
        return compactToMiniteFixJar;
    }

    public String getCompactToSecFixJar()
    {
        return compactToSecFixJar;
    }

    public static DateFormatStr getDateFormatStr()
    {
        return dateFormatStr;
    }
}
