package tech.datatype;


public class IndexingSystem
{
  public interface Forward
  {
    int globalToLocal(int global_idx);
  }
  public interface Backward
  {
    IntIter localToGlobal(int local_idx);
  }
}
