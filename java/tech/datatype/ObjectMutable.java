package tech.datatype;


public interface ObjectMutable extends MutableRemove
{
  void insert(int idx, Object value);
  void append(Object value);
}
