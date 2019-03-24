package tech.datatype;


public interface LongReader extends IOBase, Iterable
{
  long read(int idx);
}
