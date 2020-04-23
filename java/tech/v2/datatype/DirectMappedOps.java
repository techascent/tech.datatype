package tech.v2.datatype;

import com.sun.jna.Pointer;

public class DirectMappedOps
{
  public static native Pointer memset(Pointer data, int val, int numBytes);
  public static native Pointer memcpy(Pointer dst, Pointer src, int numBytes);
}
