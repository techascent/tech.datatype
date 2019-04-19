package tech.v2.datatype;

import clojure.lang.IFn;

public class BinaryOperators
{
  public interface ByteBinary extends Datatype, IFn
  {
    byte op(byte lhs, byte rhs);
    default byte finalize(byte accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs) {
      return op((byte) lhs, (byte) rhs);
    }
  };
  public interface ShortBinary extends Datatype, IFn
  {
    short op(short lhs, short rhs);
    default short finalize(short accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs) {
      return op((short) lhs, (short) rhs);
    }
  };
  public interface IntBinary extends Datatype, IFn
  {
    int op(int lhs, int rhs);
    default int finalize(int accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs)
    {
      return op((int) lhs, (int) rhs);
    }
  };
  public interface LongBinary extends Datatype, IFn
  {
    long op(long lhs, long rhs);
    default long finalize(long accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs) {
      return op((long) lhs, (long) rhs);
    }
  };
  public interface FloatBinary extends Datatype, IFn
  {
    float op(float lhs, float rhs);
    default float finalize(float accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs) {
      return op((float) lhs, (float) rhs);
    }
  };
  public interface DoubleBinary extends Datatype, IFn
  {
    double op(double lhs, double rhs);
    default double finalize(double accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs) {
      return op((double) lhs, (double) rhs);
    }
  };
  public interface BooleanBinary extends Datatype, IFn
  {
    boolean op(boolean lhs, boolean rhs);
    default boolean finalize(boolean accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs)
    {
      return op((boolean) lhs, (boolean) rhs);
    }
  };
  public interface ObjectBinary extends Datatype, IFn
  {
    Object op(Object lhs, Object rhs);
    default Object finalize(Object accum, long num_elems) {
      return accum;
    }
    default Object invoke(Object lhs, Object rhs)
    {
      return op(lhs, rhs);
    }
  };
}
