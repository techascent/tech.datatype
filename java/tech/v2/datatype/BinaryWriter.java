package tech.v2.datatype;


public interface BinaryWriter
{
  void writeBoolean(long offset, boolean data);
  void writeByte(long offset, byte data);
  void writeShort(long offset, short data);
  void writeInt(long offset, short data);
  void writeLong(long offset, long data);
  void writeFloat(long offset, float data);
  void writeDouble(long offset, double data);
}
