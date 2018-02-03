package cn.migu.streaming.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * 通用类类型加载工具类
 *
 * @author zhaocan
 * @version [版本号, 2016年9月12日]
 * @see  [相关类/方法]
 * @since [产品/模块版本]
 */
public final class ClassUtils
{

    private static final Class<?>[] NO_TYPES = new Class<?>[0];

    private static final Object[] NO_ARGS = new Object[0];

    private ClassUtils()
    {
    }

    /**
     * @param className 类名称
     * @param <T> 期望返回的类型
     * @return 期望的类类型
     */
    public static <T> Class<T> loadClass(String className)
    {
        try
        {
            @SuppressWarnings("unchecked") Class<T> theClass = (Class<T>)forName(className);
            return theClass;
        }
        catch (ClassNotFoundException cnfe)
        {
            throw new IllegalStateException("No valid " + className + " exists", cnfe);
        }
    }

    /**
     * @param className 类型名
     * @param superClass 父类类类型
     * @return 期望的类类型
     */
    public static <T> Class<? extends T> loadClass(String className, Class<T> superClass)
    {
        try
        {
            return forName(className).asSubclass(superClass);
        }
        catch (ClassNotFoundException cnfe)
        {
            throw new IllegalStateException("No valid " + superClass + " binding exists", cnfe);
        }
    }

    /**
     * 类是否已加载
     * @param implClassName 实现类名
     * @return 如果类已被加载进jvm返回true, 否则返回false
     */
    public static boolean classExists(String implClassName)
    {
        try
        {
            forName(implClassName);
            return true;
        }
        catch (ClassNotFoundException ignored)
        {
            return false;
        }
    }

    private static Class<?> forName(String implClassName)
        throws ClassNotFoundException
    {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null)
        {
            cl = ClassUtils.class.getClassLoader();
        }
        return Class.forName(implClassName, true, cl);
    }

    /**
     * 加载生成类对象
     *
     * @param clazz 类类型
     * @return 类对象
     */
    public static <T> T loadInstanceOf(Class<T> clazz)
    {
        return loadInstanceOf(clazz.getName(), clazz);
    }

    /**
     * 无参构造函数
     *
     * @param implClassName 类名称
     * @param superClass 父类类型
     * @return 类对象
     */
    public static <T> T loadInstanceOf(String implClassName, Class<T> superClass)
    {
        return loadInstanceOf(implClassName, superClass, NO_TYPES, NO_ARGS);
    }

    /**
     * 指定构造函数生成类对象
     *
     * @param implClassName 类名称
     * @param superClass 父类类型
     * @param constructorTypes 构造函数参数类型
     * @param constructorArgs 构造函数参数值
     * @return 类对象
     */
    public static <T> T loadInstanceOf(String implClassName, Class<T> superClass, Class<?>[] constructorTypes,
        Object[] constructorArgs)
    {
        try
        {
            Class<? extends T> configClass = loadClass(implClassName, superClass);
            Constructor<? extends T> constructor = configClass.getConstructor(constructorTypes);
            return constructor.newInstance(constructorArgs);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException e)
        {
            throw new IllegalArgumentException("No valid " + superClass + " binding exists", e);
        }
        catch (InvocationTargetException ite)
        {
            throw new IllegalStateException("Could not instantiate " + superClass + " due to exception",
                ite.getCause());
        }
    }

}
