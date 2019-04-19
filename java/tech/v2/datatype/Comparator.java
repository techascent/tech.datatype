package tech.v2.datatype;

import it.unimi.dsi.fastutil.bytes.ByteComparator;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.floats.FloatComparator;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;


//Helper class to make building typed comparators easier.
public class Comparator
{
  public interface ByteComp extends ByteComparator
  {
    default int compare(byte lhs, byte rhs)
    {
      return compareBytes(lhs, rhs);
    }
    int compareBytes(byte lhs, byte rhs);
  }
  public interface ShortComp extends ShortComparator
  {
    default int compare(short lhs, short rhs)
    {
      return compareShorts(lhs, rhs);
    }
    int compareShorts(short lhs, short rhs);
  }
  public interface IntComp extends IntComparator
  {
    default int compare(int lhs, int rhs)
    {
      return compareInts(lhs, rhs);
    }
    public int compareInts(int lhs, int rhs);
  }
  public interface LongComp extends LongComparator
  {
    default int compare(long lhs, long rhs)
    {
      return compareLongs(lhs, rhs);
    }
    public int compareLongs(long lhs, long rhs);
  }
  public interface FloatComp extends FloatComparator
  {
    default int compare(float lhs, float rhs)
    {
      return compareFloats(lhs, rhs);
    }
    public int compareFloats(float lhs, float rhs);
  }
  public interface DoubleComp extends DoubleComparator
  {
    default int compare(double lhs, double rhs)
    {
      return compareDoubles(lhs, rhs);
    }
    public int compareDoubles(double lhs, double rhs);
  }
};
