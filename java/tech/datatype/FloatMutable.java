package tech.datatype;


public interface FloatMutable extends MutableRemove
{
  void insert(int idx, float value);
  void append(float value);
}
