package cn.migu.streaming.timer;

/**
 *
 *
 * author  zhaocan
 * version  [版本号, 2016/9/27]
 * see  [相关类/方法]
 * since  [产品/模块版本]
 */
@FunctionalInterface interface TimerFunction<P1, P2, P3, R>
{
    R apply(P1 p1, P2 p2, P3 p3);
}
