package tech.datatype;


public interface DoubleMutable extends MutableRemove
{
  void insert(int idx, double value);
  void append(double value);
}
