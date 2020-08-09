package tech.v2.datatype;
/**
 * In order to use java.util.Map classes well you often need to use Functions
 * and BiFunctions. The problem is that you cannot pass more context into these objects
 * so a simple extension to these objects is to provide more context.
 * In all cases, context is the first argument to supplied function.
 */

import clojure.lang.IFn;
import java.util.function.Function;
import java.util.function.BiFunction;


public class Functions
{
  public class CtxFunction implements Function
  {
    public final IFn fn;
    public Object context;
    public CtxFunction(IFn _fn)
    {
      this.fn = _fn;
      this.context = null;
    }
    public void setContext(Object ctx)
    {
      this.context = ctx;
    }
    public Object apply( Object arg)
    {
      return fn.invoke(context, arg);
    }
  }

  public class CtxBiFunction implements BiFunction
  {
    public final IFn fn;
    public Object context;
    public CtxBiFunction(IFn _fn)
    {
      this.fn = _fn;
      this.context = null;
    }
    public void setContext(Object ctx)
    {
      this.context = ctx;
    }
    public Object apply(Object arg1, Object arg2)
    {
      return fn.invoke(context, arg1, arg2);
    }
  }

  public interface LongBiFunction
  {
    public Object apply(long arg1, Object arg2);
  }

  public static class LongCtxFunction implements Function
  {
    public final LongBiFunction fn;
    public long context;
    public LongCtxFunction(LongBiFunction _fn)
    {
      this.fn = _fn;
    }
    public void setContext(long ctx)
    {
      this.context = ctx;
    }
    public Object apply(Object arg1)
    {
      return fn.apply(context, arg1);
    }
  }

  public interface LongTriFunction
  {
    public Object apply(long arg1, Object arg2, Object arg3);
  }

  public static class LongCtxBiFunction implements BiFunction
  {
    public final LongTriFunction fn;
    public long context;
    public LongCtxBiFunction(LongTriFunction _fn)
    {
      this.fn = _fn;
    }
    public void setContext(long ctx)
    {
      this.context = ctx;
    }
    public Object apply(Object arg1, Object arg2)
    {
      return fn.apply(context, arg1, arg2);
    }
  }
}
