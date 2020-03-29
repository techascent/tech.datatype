package tech.v2.datatype;


public interface BinaryReader
{
  boolean readBoolean(long offset);
  byte readByte(long offset);
  short readShort(long offset);
  int readInt(long offset);
  long readLong(long offset);
  float readFloat(long offset);
  double readDouble(long offset);
}
