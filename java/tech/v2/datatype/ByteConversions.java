package tech.v2.datatype;


public class ByteConversions
{
  public static byte byteFromBytes(byte val) { return val; }
  public static byte byteFromReader(ByteReader reader, long offset) {
    return reader.read(0+offset);
  }
  public static void byteToWriter(byte val, ByteWriter writer, long offset){
    writer.write(0+offset, val);
  }
  public static short shortFromBytesLE(byte b1, byte b2) {
    return (short) ((b1 & 0xFF) | ((b2 & 0xFF) << 8));
  }
  public static short shortFromBytesBE(byte b1, byte b2) {
    return shortFromBytesLE( b2, b1 );
  }
  public static short shortFromReaderLE (ByteReader reader, long offset) {
    return shortFromBytesLE(reader.read(0+offset), reader.read(1+offset));
  }
  public static short shortFromReaderBE (ByteReader reader, long offset) {
    return shortFromBytesLE(reader.read(1+offset), reader.read(0+offset));
  }
  public static void shortToWriterLE (short value, ByteWriter writer, long offset) {
    writer.write(0+offset, (byte)(value & 0xFF));
    writer.write(1+offset, (byte)((value >> 8) & 0xFF));
  }
  public static void shortToWriterBE (short value, ByteWriter writer, long offset) {
    writer.write(1+offset, (byte)(value & 0xFF));
    writer.write(0+offset, (byte)((value >> 8) & 0xFF));
  }

  public static int intFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return (int) ((b1 & 0xFF) |
		  ((b2 & 0xFF) << 8) |
		  ((b3 & 0xFF) << 16) |
		  ((b4 & 0xFF) << 24));
  }
  public static int intFromBytesBE(byte b1, byte b2, byte b3, byte b4) {
    return intFromBytesLE( b4, b3, b2, b1 );
  }
  public static int intFromReaderLE (ByteReader reader, long offset) {
    return intFromBytesLE(reader.read(0+offset), reader.read(1+offset),
			  reader.read(2+offset), reader.read(3+offset));
  }
  public static int intFromReaderBE (ByteReader reader, long offset) {
    return intFromBytesLE(reader.read(3+offset), reader.read(2+offset),
			  reader.read(1+offset), reader.read(0+offset));
  }
  public static void intToWriterLE (int value, ByteWriter writer, long offset) {
    writer.write(0+offset, (byte)(value & 0xFF));
    writer.write(1+offset, (byte)((value >> 8) & 0xFF));
    writer.write(2+offset, (byte)((value >> 16) & 0xFF));
    writer.write(3+offset, (byte)((value >> 24) & 0xFF));
  }
  public static void intToWriterBE (int value, ByteWriter writer, long offset) {
    writer.write(3+offset, (byte)(value & 0xFF));
    writer.write(2+offset, (byte)((value >> 8) & 0xFF));
    writer.write(1+offset, (byte)((value >> 16) & 0xFF));
    writer.write(0+offset, (byte)((value >> 24) & 0xFF));
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
  public static long longFromReaderLE (ByteReader reader, long offset) {
    return longFromBytesLE(reader.read(0+offset), reader.read(1+offset),
			   reader.read(2+offset), reader.read(3+offset),
			   reader.read(4+offset), reader.read(5+offset),
			   reader.read(6+offset), reader.read(7+offset));
  }
  public static long longFromReaderBE (ByteReader reader, long offset) {
    return longFromBytesLE(reader.read(7+offset), reader.read(6+offset),
			   reader.read(5+offset), reader.read(4+offset),
			   reader.read(3+offset), reader.read(2+offset),
			   reader.read(1+offset), reader.read(0+offset));
  }
  public static void longToWriterLE (long value, ByteWriter writer, long offset) {
    writer.write(0+offset, (byte)(value & 0xFF));
    writer.write(1+offset, (byte)((value >> 8) & 0xFF));
    writer.write(2+offset, (byte)((value >> 16) & 0xFF));
    writer.write(3+offset, (byte)((value >> 24) & 0xFF));
    writer.write(4+offset, (byte)((value >> 32) & 0xFF));
    writer.write(5+offset, (byte)((value >> 40) & 0xFF));
    writer.write(6+offset, (byte)((value >> 48) & 0xFF));
    writer.write(7+offset, (byte)((value >> 56) & 0xFF));
  }
  public static void longToWriterBE (long value, ByteWriter writer, long offset) {
    writer.write(7+offset, (byte)(value & 0xFF));
    writer.write(6+offset, (byte)((value >> 8) & 0xFF));
    writer.write(5+offset, (byte)((value >> 16) & 0xFF));
    writer.write(4+offset, (byte)((value >> 24) & 0xFF));
    writer.write(3+offset, (byte)((value >> 32) & 0xFF));
    writer.write(2+offset, (byte)((value >> 40) & 0xFF));
    writer.write(1+offset, (byte)((value >> 48) & 0xFF));
    writer.write(0+offset, (byte)((value >> 56) & 0xFF));
  }

  public static float floatFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesLE(b1,b2,b3,b4));
  }
  public static float floatFromBytesBE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesBE(b1,b2,b3,b4));
  }
  public static float floatFromReaderLE (ByteReader reader, long offset) {
    return Float.intBitsToFloat(intFromReaderLE(reader,offset));
  }
  public static float floatFromReaderBE (ByteReader reader,long offset) {
    return Float.intBitsToFloat(intFromReaderBE(reader,offset));
  }
  public static void floatToWriterLE (float value, ByteWriter writer, long offset) {
    intToWriterLE(Float.floatToRawIntBits(value), writer, offset);
  }
  public static void floatToWriterBE (float value, ByteWriter writer, long offset) {
    intToWriterBE(Float.floatToRawIntBits(value), writer, offset);
  }

  public static double doubleFromBytesLE(byte b1, byte b2, byte b3, byte b4,
					 byte b5, byte b6, byte b7, byte b8) {

    return Double.longBitsToDouble(longFromBytesLE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromBytesBE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8) {
    return Double.longBitsToDouble(longFromBytesBE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromReaderLE (ByteReader reader, long offset) {
    return Double.longBitsToDouble(longFromReaderLE(reader,offset));
  }
  public static double doubleFromReaderBE (ByteReader reader, long offset) {
    return Double.longBitsToDouble(longFromReaderBE(reader, offset));
  }
  public static void doubleToWriterLE (double value, ByteWriter writer, long offset) {
    longToWriterLE(Double.doubleToRawLongBits(value), writer, offset);
  }
  public static void doubleToWriterBE (double value, ByteWriter writer, long offset) {
    longToWriterBE(Double.doubleToRawLongBits(value), writer, offset);
  }
}
