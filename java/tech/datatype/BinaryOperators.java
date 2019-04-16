package tech.datatype;

import clojure.lang.IFn;

public class BinaryOperators
{
  public interface ByteBinary extends Datatype, IFn
  {
    byte op(byte lhs, byte rhs);
  };
  public interface ShortBinary extends Datatype, IFn
  {
    short op(short lhs, short rhs);
  };
  public interface IntBinary extends Datatype, IFn
  {
    int op(int lhs, int rhs);
  };
  public interface LongBinary extends Datatype, IFn
  {
    long op(long lhs, long rhs);
  };
  public interface FloatBinary extends Datatype, IFn
  {
    float op(float lhs, float rhs);
  };
  public interface DoubleBinary extends Datatype, IFn
  {
    double op(double lhs, double rhs);
  };
  public interface BooleanBinary extends Datatype, IFn
  {
    boolean op(boolean lhs, boolean rhs);
  };
  public interface ObjectBinary extends Datatype, IFn
  {
    Object op(Object lhs, Object rhs);
  };
}
