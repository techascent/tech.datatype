package tech.datatype;
import java.nio.*;

public interface ByteReader extends IOBase, Iterable
{
  byte read(int idx);
}
