package cn.migu.streaming.common;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * 方法调用通用类
 * 
 * @author  zhaocan
 * @version  [版本号, 2015-10-13]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public class MethodCallUtils
{
    /**
 *
 * 根据属性名调用类set方法
 * @param clazz 类名
 * @param fieldName 属性名
 * @param args 属性参数值
 * @return
 * @throws Exception
 * @see [类、类#方法、类#成员]
 */
    public static Object invokeSetMethod(Object clazz, String fieldName, Object[] args)
    {
        String methodName =
            StringUtils.join(fieldName.substring(0, 1).toUpperCase(), fieldName.substring(1));

        Method method = null;
        try
        {
            Class<?>[] parameterTypes = new Class[1];
            Class<?> c = Class.forName(clazz.getClass().getName());
            Field field = c.getDeclaredField(fieldName);
            parameterTypes[0] = field.getType();
            method = c.getDeclaredMethod("set" + methodName, parameterTypes);
            return method.invoke(clazz, args);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return null;
    }
    
    /**
     * 
     * 根据属性名调用类get方法
     * @param clazz 类型
     * @param fieldName 属性名
     * @return
     * @throws Exception 
     * @see [类、类#方法、类#成员]
     */
    public static Object invokeGetMethod(Object clazz, String fieldName)
    {
        String methodName =
            StringUtils.join(fieldName.substring(0, 1).toUpperCase(), fieldName.substring(1));
        
        Method method = null;
        try
        {
            method = Class.forName(clazz.getClass().getName()).getDeclaredMethod("get" + methodName);
            return method.invoke(clazz);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return null;
    }
}
