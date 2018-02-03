package cn.migu.streaming.common;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Function的简单封装
 *
 * @author  zhaocan
 * @version  [版本号, 2016年9月12日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public final class Functions
{
    
    private Functions()
    {
    }

    public static <T> Function2<T, T, T> last()
    {
        return new Function2<T, T, T>()
        {
            @Override
            public T call(T current, T next)
            {
                return next;
            }
        };
    }
    
    public static <T> VoidFunction<T> noOp()
    {
        return new VoidFunction<T>()
        {
            @Override
            public void call(T t)
            {
                // do nothing
            }
        };
    }
    
    public static <T> Function<T, T> identity()
    {
        return new Function<T, T>()
        {
            @Override
            public T call(T t)
            {
                return t;
            }
        };
    }
    
}
