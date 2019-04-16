package tech.datatype;


public class ReduceOperators
{
  public interface ByteReduce extends Datatype
  {
    byte update(byte accum, byte next);
    byte finalize(byte accum, int num_elems);
  };
  public interface ShortReduce extends Datatype
  {
    short update(short accum, short next);
    short finalize(short accum, int num_elems);
  };
  public interface IntReduce extends Datatype
  {
    int update(int accum, int next);
    int finalize(int accum, int num_elems);
  };
  public interface LongReduce extends Datatype
  {
    long update(long accum, long next);
    long finalize(long accum, int num_elems);
  };
  public interface FloatReduce extends Datatype
  {
    float update(float accum, float next);
    float finalize(float accum, int num_elems);
  };
  public interface DoubleReduce extends Datatype
  {
    double update(double accum, double next);
    double finalize(double accum, int num_elems);
  };
  public interface BooleanReduce extends Datatype
  {
    boolean update(boolean accum, boolean next);
    boolean finalize(boolean accum, int num_elems);
  };
  public interface ObjectReduce extends Datatype
  {
    Object update(Object accum, Object next);
    Object finalize(Object accum, int num_elems);
  };
}
