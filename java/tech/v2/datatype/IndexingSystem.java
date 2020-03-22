package tech.v2.datatype;


public class IndexingSystem
{
  public interface Backward
  {
    //Returns java Long or an Iterable
    Object localToGlobal(long local_idx);
  }
}
