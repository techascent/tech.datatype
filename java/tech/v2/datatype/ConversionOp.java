package tech.v2.datatype;

import clojure.lang.Keyword;

public class ConversionOp
{
  public interface LongObj
  {
    public Object longToObj(long item);
  };
  public interface ObjLong
  {
    public long longToObj(Object item);
  };
  public interface IntObj
  {
    public Object intToObj(int item);
  };
  public interface ObjInt
  {
    public int objToInt(Object item);
  };
};
