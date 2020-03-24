package tech.v2.datatype;


public class ByteConversions
{
  public static byte byteFromBytes(byte val) { return val; }
  public static byte byteFromReader(ByteReader reader) { return reader.read(0); }
  public static void byteToWriter(byte val, ByteWriter writer) { writer.write(0, val); }
  public static short shortFromBytesLE(byte b1, byte b2) { return (short) ((b1 & 0xFF) | ((b2 & 0xFF) << 8));}
  public static short shortFromBytesBE(byte b1, byte b2) { return shortFromBytesLE( b2, b1 ); }
  public static short shortFromReaderLE (ByteReader reader) {
    return shortFromBytesLE(reader.read(0), reader.read(1));
  }
  public static short shortFromReaderBE (ByteReader reader) {
    return shortFromBytesLE(reader.read(1), reader.read(0));
  }
  public static void shortToByteWriterLE (short value, ByteWriter writer) {
    writer.write(0, (byte)(value & 0xFF));
    writer.write(1, (byte)((value >> 8) & 0xFF));
  }
  public static void shortToByteWriterBE (short value, ByteWriter writer) {
    writer.write(1, (byte)(value & 0xFF));
    writer.write(0, (byte)((value >> 8) & 0xFF));
  }

  // public static short shortFromBytesLE (byte b1, byte b2 ) { return (short) (b1 + b2 << 8); }
  // public static short shortFromBytesBE (byte b1, byte b2 ) { return (short) (b1 << 8 + b2); }

}
