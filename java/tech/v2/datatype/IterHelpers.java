package tech.v2.datatype;

import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import clojure.lang.Keyword;
import java.util.Iterator;



public class IterHelpers
{
  public static Object keywordOrDefault(Object keyword, String defValue)
  {
    if (keyword != null)
      return keyword;
    else
      return Keyword.intern(null, defValue);
  }
  public static class ByteIterConverter implements ByteIter
  {
    ByteIterator iter;
    byte current;
    Object dtype;
    public ByteIterConverter(ByteIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "int8");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public byte nextByte() {
      current = iter.nextByte();
      return current;
    }
    public byte current() { return current; }
  };
  public static class ShortIterConverter implements ShortIter
  {
    ShortIterator iter;
    short current;
    Object dtype;
    public ShortIterConverter(ShortIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "int16");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public short nextShort() {
      current = iter.nextShort();
      return current;
    }
    public short current() { return current; }
  };
  public static class IntIterConverter implements IntIter
  {
    IntIterator iter;
    int current;
    Object dtype;
    public IntIterConverter(IntIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "int32");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public int nextInt() {
      current = iter.nextInt();
      return current;
    }
    public int current() { return current; }
  };
  public static class LongIterConverter implements LongIter
  {
    LongIterator iter;
    long current;
    Object dtype;
    public LongIterConverter(LongIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "int64");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public long nextLong() {
      current = iter.nextLong();
      return current;
    }
    public long current() { return current; }
  };
  public static class FloatIterConverter implements FloatIter
  {
    FloatIterator iter;
    float current;
    Object dtype;
    public FloatIterConverter(FloatIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "float32");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public float nextFloat() {
      current = iter.nextFloat();
      return current;
    }
    public float current() { return current; }
  };
  public static class DoubleIterConverter implements DoubleIter
  {
    DoubleIterator iter;
    double current;
    Object dtype;
    public DoubleIterConverter(DoubleIterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "float64");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public double nextDouble() {
      current = iter.nextDouble();
      return current;
    }
    public double current() { return current; }
  };
  public static class ObjectIterConverter implements ObjectIter
  {
    Iterator iter;
    Object current;
    Object dtype;
    public ObjectIterConverter(Iterator _iter, Object _dtype)
    {
      iter =_iter;
      dtype = keywordOrDefault(_dtype, "object");
    }
    public Object getDatatype() { return dtype; }
    public boolean hasNext() { return iter.hasNext(); }
    public Object next() {
      current = iter.next();
      return current;
    }
    public Object current() { return current; }
  };

}
