package cn.migu.streaming.timer;

import cn.migu.streaming.common.DateUtils;
import cn.migu.streaming.model.FileCleanConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2016/9/27]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
public class TimerRefresh
{
    private static final Logger log = LoggerFactory.getLogger("streaming");

    private static TimerRefresh instance;

    private TimerRefresh()
    {

    }

    public static TimerRefresh getInstance()
    {
        if (null == instance)
        {
            instance = new TimerRefresh();
        }
        return instance;
    }

    /**
     * 刷新内存表
     * @param fileConf
     */
    public void refresh(FileCleanConf fileConf)
    {
        String timePoint = fileConf.getResetTime();
        String refreshDate = fileConf.getRefreshDate();

        String tDay = DateUtils.getSpecFormatDate("yyyy-MM-dd");

        TimerFunction<FileCleanConf, Void, Void, Void> func = (a, b, c) ->
        {
            log.info("重置内存unbound table");
            a.setFullData(null);
            return null;
        };

        boolean refreshFlag = this.timerRefresh(timePoint, refreshDate, tDay, func, fileConf, null, null);
        if (refreshFlag)
        {
            fileConf.setRefreshDate(tDay);
        }

    }

    /**
     * 按配置时间刷新
     * @param timePoint
     * @param refreshDate
     * @param tDay
     * @param func
     * @param params
     */
    private boolean timerRefresh(String timePoint, String refreshDate, String tDay, TimerFunction func, Object... params)
    {
        log.info("resetTimePoint:{},refreshDate:{},currentDay:{}", timePoint, refreshDate, tDay);
        if (StringUtils.isNotEmpty(timePoint))
        {
            if (!StringUtils.equals(tDay, refreshDate))
            {
                String[] ms = StringUtils.split(timePoint, ":");
                if (ms.length >= 2 && NumberUtils.isNumber(ms[0]) && NumberUtils.isNumber(ms[1]))
                {
                    Calendar cal = Calendar.getInstance();
                    int hour = cal.get(Calendar.HOUR_OF_DAY);
                    int minute = cal.get(Calendar.MINUTE);

                    int _hour = Integer.valueOf(ms[0]);
                    int _minute = Integer.valueOf(ms[1]);

                    if (hour > _hour || (hour == _hour && minute > _minute))
                    {
                        func.apply(params[0], params[1], params[2]);

                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static void main(String[] args)
    {
        String timePoint = "00:00";
        String refreshDate = "2016-10-12";

        String tDay = DateUtils.getSpecFormatDate("yyyy-MM-dd");

        TimerFunction<FileCleanConf, Void, Void, Void> func = (a, b, c) ->
        {
            System.out.println("重置内存unbound table");
            a.setFullData(null);
            return null;
        };

        FileCleanConf fileConf = new FileCleanConf();
        boolean resetFlag = TimerRefresh.getInstance().timerRefresh(timePoint, refreshDate, tDay, func, new FileCleanConf(), null, null);
        if (resetFlag)
        {
            fileConf.setRefreshDate(tDay);
        }

        System.out.println(fileConf.getRefreshDate());
    }
}
