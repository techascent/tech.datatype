package tech.v2.datatype;


public class IndexingSystem
{
  //Forward is an IntReader.
  public interface Forward extends LongReader
  {
  }
  public interface Backward
  {
    //Returns an iterable that when asked returns an IntIter or nil
    Object localToGlobal(int local_idx);
  }
}
