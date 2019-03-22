package tech.datatype;
import java.nio.*;

public interface ByteReader extends Datatype
{
  byte read(int idx);
}
