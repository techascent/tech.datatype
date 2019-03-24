package tech.datatype;


public interface ShortReader extends IOBase, Iterable
{
  short read(int idx);
}
