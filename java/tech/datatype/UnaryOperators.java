package tech.datatype;

import clojure.lang.IFn;

public class UnaryOperators
{
  public interface ByteUnary extends Datatype, IFn
  {
    byte op(byte arg);
  };
  public interface ShortUnary extends Datatype, IFn
  {
    short op(short arg);
  };
  public interface IntUnary extends Datatype, IFn
  {
    int op(int arg);
  };
  public interface LongUnary extends Datatype, IFn
  {
    long op(long arg);
  };
  public interface FloatUnary extends Datatype, IFn
  {
    float op(float arg);
  };
  public interface DoubleUnary extends Datatype, IFn
  {
    double op(double arg);
  };
  public interface BooleanUnary extends Datatype, IFn
  {
    boolean op(boolean arg);
  };
  public interface ObjectUnary extends Datatype, IFn
  {
    Object op(Object arg);
  };
}
