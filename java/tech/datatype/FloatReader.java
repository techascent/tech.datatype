package tech.datatype;


public interface FloatReader extends IOBase, Iterable
{
  float read(int idx);
}
