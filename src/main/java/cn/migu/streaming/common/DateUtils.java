package cn.migu.streaming.common;

import org.datanucleus.util.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类
 *
 * @author zhaocan
 * @version [版本号, 2016年9月12日]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public final class DateUtils
{
    /**
     * 获取指定格式日期
     * @param format 日期格式字符串
     * @return
     */
    public static String getSpecFormatDate(String format)
    {
        Date currentDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String specifiedDay = sdf.format(currentDate);

        return specifiedDay;
    }

    /**
     * 获取当天日期
     * @param format
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getCurrentDate(String format)
    {

        Date currentDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String specifiedDay = sdf.format(currentDate);

        return specifiedDay;
    }

    /**
     * 获取日期前n天
     * @param n
     * @param format
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getBeforeNDay(int n, String format)
    {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try
        {
            date = new SimpleDateFormat(format).parse(getCurrentDate(format));

        }
        catch (ParseException e)
        {
            e.printStackTrace();
            return null;
        }
        c.setTime(date);
        int _day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, _day - n);

        String dayBefore = new SimpleDateFormat(format).format(c.getTime());
        return dayBefore;
    }

    /**
     * 当天日期退后n天
     * @param n
     * @param format
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getAfterNDay(int n, String format)
    {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try
        {
            date = new SimpleDateFormat(format).parse(getCurrentDate(format));
        }
        catch (ParseException e)
        {
            e.printStackTrace();
            return null;
        }
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day + n);

        String dayAfter = new SimpleDateFormat(format).format(c.getTime());
        return dayAfter;
    }

    /**
     * 日期比较
     * @param dateStr_1
     * @param dateStr_2
     * @param formatStr
     * @return
     */
    public static long compareDate(String dateStr_1, String dateStr_2, String formatStr)
    {
        DateFormat df = new SimpleDateFormat(formatStr);

        try
        {
            Date dt1 = df.parse(dateStr_1);
            Date dt2 = df.parse(dateStr_2);

            return dt1.getTime() - dt2.getTime();
        }
        catch (ParseException e)
        {
            return 0;
        }

    }

    /**
     * 有效日期格式判定
     * @param dateStr
     * @param format
     * @return
     */
    public static boolean validateDateFormat(String dateStr, String format)
    {
        if (StringUtils.isEmpty(dateStr) || StringUtils.isEmpty(format))
        {
            return true;
        }
        else
        {
            SimpleDateFormat sdfrmt = new SimpleDateFormat(format);
            sdfrmt.setLenient(false);
            try
            {
                sdfrmt.parse(dateStr);
            }
            catch (ParseException e)
            {
                return false;
            }
            return true;
        }
    }

    public static void main(String[] args)
    {
        //String ss = getSpecFormatDate("HHmmss");
        System.out.println(validateDateFormat("2017年01月03日 17:16:00", "yyyy年MM月dd日 HH:mm:ss"));
    }
}
