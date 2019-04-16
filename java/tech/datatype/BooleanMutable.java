package tech.datatype;

public interface BooleanMutable extends MutableRemove
{
  void insert(int idx, boolean value);
  void append(boolean value);
}
