package tech.v2.datatype;


public interface BinaryWriter
{
  void writeBoolean(boolean data, long offset);
  void writeByte(byte data, long offset);
  void writeShort(short data, long offset);
  void writeInt(short data, long offset);
  void writeLong(long data, long offset);
  void writeFloat(float data, long offset);
  void writeDouble(double data, long offset);
}
