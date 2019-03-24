package tech.datatype;


public interface ObjectReader extends IOBase, Iterable
{
  Object read(int idx);
};
