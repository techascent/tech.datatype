package tech.datatype;
import java.nio.*;

public interface ByteReader extends IOBase
{
  byte read(int idx);
}
