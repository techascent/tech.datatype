package tech.datatype;
import java.nio.*;

public interface ByteReader
{
  byte read(int idx);
  void readBlock(int offset, ByteBuffer destination);
  void readIndexes(IntBuffer indexes, ByteBuffer destination);
}
