package tech.datatype;


public interface LongMutable extends MutableRemove
{
  void insert(int idx, long value);
  void append(long value);
}
