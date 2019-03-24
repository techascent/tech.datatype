package tech.datatype;

import clojure.lang.IFn;

public class ReduceOperators
{
  public interface ByteReduce extends Datatype
  {
    byte initialize(byte first_value);
    byte update(byte accum, byte next);
    byte finalize(byte accum, int num_elems);
  };
  public interface ShortReduce extends Datatype
  {
    short initialize(short first_value);
    short update(short accum, short next);
    short finalize(short accum, int num_elems);
  };
  public interface IntReduce extends Datatype
  {
    int initialize(int first_value);
    int update(int accum, int next);
    int finalize(int accum, int num_elems);
  };
  public interface LongReduce extends Datatype
  {
    long initialize(long first_value);
    long update(long accum, long next);
    long finalize(long accum, int num_elems);
  };
  public interface FloatReduce extends Datatype
  {
    float initialize(float first_value);
    float update(float accum, float next);
    float finalize(float accum, int num_elems);
  };
  public interface DoubleReduce extends Datatype
  {
    double initialize(double first_value);
    double update(double accum, double next);
    double finalize(double accum, int num_elems);
  };
  public interface BooleanReduce extends Datatype
  {
    boolean initialize(boolean first_value);
    boolean update(boolean accum, boolean next);
    boolean finalize(boolean accum, int num_elems);
  };
  public interface ObjectReduce extends Datatype
  {
    Object initialize(Object first_value);
    Object update(Object accum, Object next);
    Object finalize(Object accum, int num_elems);
  };
}
