package tech.v2.datatype;


public class BooleanOp
{
  public interface ByteUnary
  {
    boolean op(byte value);
  }
  public interface ShortUnary
  {
    boolean op(short value);
  }
  public interface IntUnary
  {
    boolean op(int value);
  }
  public interface LongUnary
  {
    boolean op(long value);
  }
  public interface FloatUnary
  {
    boolean op(float value);
  }
  public interface DoubleUnary
  {
    boolean op(double value);
  }
  public interface ObjectUnary
  {
    boolean op(Object value);
  }


  public interface ByteBinary
  {
    boolean op(byte lhs, byte rhs);
  }
  public interface ShortBinary
  {
    boolean op(short lhs, short rhs);
  }
  public interface IntBinary
  {
    boolean op(int lhs, int rhs);
  }
  public interface LongBinary
  {
    boolean op(long lhs, long rhs);
  }
  public interface FloatBinary
  {
    boolean op(float lhs, float rhs);
  }
  public interface DoubleBinary
  {
    boolean op(double lhs, double rhs);
  }
  public interface ObjectBinary
  {
    boolean op(Object lhs, Object rhs);
  }
}
