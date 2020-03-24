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
  public static void shortToWriterLE (short value, ByteWriter writer) {
    writer.write(0, (byte)(value & 0xFF));
    writer.write(1, (byte)((value >> 8) & 0xFF));
  }
  public static void shortToWriterBE (short value, ByteWriter writer) {
    writer.write(1, (byte)(value & 0xFF));
    writer.write(0, (byte)((value >> 8) & 0xFF));
  }

  public static int intFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return (int) ((b1 & 0xFF) |
		  ((b2 & 0xFF) << 8) |
		  ((b3 & 0xFF) << 16) |
		  ((b4 & 0xFF) << 24));
  }
  public static int intFromBytesBE(byte b1, byte b2, byte b3, byte b4) { return intFromBytesLE( b4, b3, b2, b1 ); }
  public static int intFromReaderLE (ByteReader reader) {
    return intFromBytesLE(reader.read(0), reader.read(1), reader.read(2), reader.read(3));
  }
  public static int intFromReaderBE (ByteReader reader) {
    return intFromBytesLE(reader.read(3), reader.read(2), reader.read(1), reader.read(0));
  }
  public static void intToWriterLE (int value, ByteWriter writer) {
    writer.write(0, (byte)(value & 0xFF));
    writer.write(1, (byte)((value >> 8) & 0xFF));
    writer.write(2, (byte)((value >> 16) & 0xFF));
    writer.write(3, (byte)((value >> 24) & 0xFF));
  }
  public static void intToWriterBE (int value, ByteWriter writer) {
    writer.write(3, (byte)(value & 0xFF));
    writer.write(2, (byte)((value >> 8) & 0xFF));
    writer.write(1, (byte)((value >> 16) & 0xFF));
    writer.write(0, (byte)((value >> 24) & 0xFF));
  }


  public static long longFromBytesLE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8) {
    return (((long)(b1 & 0xFF)) |
	    ((long)(b2 & 0xFF) << 8) |
	    ((long)(b3 & 0xFF) << 16) |
	    ((long)(b4 & 0xFF) << 24) |
	    (((long)(b5 & 0xFF)) << 32) |
	    (((long)(b6 & 0xFF)) << 40) |
	    (((long)(b7 & 0xFF)) << 48) |
	    (((long)(b8 & 0xFF)) << 56));
  }
  public static long longFromBytesBE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8)
  { return longFromBytesLE( b8, b7, b6, b5, b4, b3, b2, b1 ); }
  public static long longFromReaderLE (ByteReader reader) {
    return longFromBytesLE(reader.read(0), reader.read(1), reader.read(2), reader.read(3),
			   reader.read(4), reader.read(5), reader.read(6), reader.read(7));
  }
  public static long longFromReaderBE (ByteReader reader) {
    return longFromBytesLE(reader.read(7), reader.read(6), reader.read(5), reader.read(4),
			   reader.read(3), reader.read(2), reader.read(1), reader.read(0));
  }
  public static void longToWriterLE (long value, ByteWriter writer) {
    writer.write(0, (byte)(value & 0xFF));
    writer.write(1, (byte)((value >> 8) & 0xFF));
    writer.write(2, (byte)((value >> 16) & 0xFF));
    writer.write(3, (byte)((value >> 24) & 0xFF));
    writer.write(4, (byte)((value >> 32) & 0xFF));
    writer.write(5, (byte)((value >> 40) & 0xFF));
    writer.write(6, (byte)((value >> 48) & 0xFF));
    writer.write(7, (byte)((value >> 56) & 0xFF));
  }
  public static void longToWriterBE (long value, ByteWriter writer) {
    writer.write(7, (byte)(value & 0xFF));
    writer.write(6, (byte)((value >> 8) & 0xFF));
    writer.write(5, (byte)((value >> 16) & 0xFF));
    writer.write(4, (byte)((value >> 24) & 0xFF));
    writer.write(3, (byte)((value >> 32) & 0xFF));
    writer.write(2, (byte)((value >> 40) & 0xFF));
    writer.write(1, (byte)((value >> 48) & 0xFF));
    writer.write(0, (byte)((value >> 56) & 0xFF));
  }

  public static float floatFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesLE(b1,b2,b3,b4));
  }
  public static float floatFromBytesBE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesBE(b1,b2,b3,b4));
  }
  public static float floatFromReaderLE (ByteReader reader) {
    return Float.intBitsToFloat(intFromReaderLE(reader));
  }
  public static float floatFromReaderBE (ByteReader reader) {
    return Float.intBitsToFloat(intFromReaderBE(reader));
  }
  public static void floatToWriterLE (float value, ByteWriter writer) {
    intToWriterLE(Float.floatToRawIntBits(value), writer);
  }
  public static void floatToWriterBE (float value, ByteWriter writer) {
    intToWriterBE(Float.floatToRawIntBits(value), writer);
  }

  public static double doubleFromBytesLE(byte b1, byte b2, byte b3, byte b4,
					 byte b5, byte b6, byte b7, byte b8) {

    return Double.longBitsToDouble(longFromBytesLE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromBytesBE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8) {
    return Double.longBitsToDouble(longFromBytesBE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromReaderLE (ByteReader reader) {
    return Double.longBitsToDouble(longFromReaderLE(reader));
  }
  public static double doubleFromReaderBE (ByteReader reader) {
    return Double.longBitsToDouble(longFromReaderBE(reader));
  }
  public static void doubleToWriterLE (double value, ByteWriter writer) {
    longToWriterLE(Double.doubleToRawLongBits(value), writer);
  }
  public static void doubleToWriterBE (double value, ByteWriter writer) {
    longToWriterBE(Double.doubleToRawLongBits(value), writer);
  }


  // public static int intFromBytesLE (byte b1, byte b2 ) { return (int) (b1 + b2 << 8); }
  // public static int intFromBytesBE (byte b1, byte b2 ) { return (int) (b1 << 8 + b2); }

}
