package tech.datatype;


public class IndexingSystem
{
  public interface Forward
  {
    int globalToLocal(int global_idx);
  }
  public interface Backward
  {
    //Returns an iterable that when asked returns an IntIter or nil
    Object localToGlobal(int local_idx);
  }
}
