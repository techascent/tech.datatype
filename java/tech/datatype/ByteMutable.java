package tech.datatype;


public interface ByteMutable extends MutableRemove
{
  void insert(int idx, byte value);
  void append(byte value);
}
