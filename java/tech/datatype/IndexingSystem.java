package tech.datatype;


public interface IndexingSystem
{
  int globalToLocal(int global_idx);
  boolean isomorphic();
  int localToGlobalInt(int local_idx);
  IntIter localToGlobal(int local_idx);
}
