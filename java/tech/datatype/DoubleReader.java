package tech.datatype;


public interface DoubleReader extends IOBase, Iterable
{
  double read(int idx);
}
