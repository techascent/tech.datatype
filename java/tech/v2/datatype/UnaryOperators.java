package tech.v2.datatype;

import clojure.lang.IFn;

public class UnaryOperators
{
  public interface ByteUnary extends Datatype, IFn
  {
    byte op(byte arg);
    default Object invoke(Object arg)
    {
      return op((byte) arg);
    }
  };
  public interface ShortUnary extends Datatype, IFn
  {
    short op(short arg);
    default Object invoke(Object arg)
    {
      return op((short) arg);
    }
  };
  public interface IntUnary extends Datatype, IFn
  {
    int op(int arg);
    default Object invoke(Object arg)
    {
      return op((int) arg);
    }
  };
  public interface LongUnary extends Datatype, IFn
  {
    long op(long arg);
    default Object invoke(Object arg)
    {
      return op((long) arg);
    }
  };
  public interface FloatUnary extends Datatype, IFn
  {
    float op(float arg);
    default Object invoke(Object arg)
    {
      return op((float) arg);
    }
  };
  public interface DoubleUnary extends Datatype, IFn
  {
    double op(double arg);
    default Object invoke(Object arg)
    {
      return op((double) arg);
    }
  };
  public interface BooleanUnary extends Datatype, IFn
  {
    boolean op(boolean arg);
    default Object invoke(Object arg)
    {
      return op((boolean) arg);
    }
  };
  public interface ObjectUnary extends Datatype, IFn
  {
    Object op(Object arg);
    default Object invoke(Object arg)
    {
      return op(arg);
    }
  };
}
