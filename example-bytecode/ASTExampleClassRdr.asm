Compiled from "ASTExampleClassRdr.java"
public class tech.v2.tensor.dimensions.ASTExampleClassRdr {
  public final long maxShapeStride0;

  public final long stride0;

  public final tech.v2.datatype.LongReader shape1;

  public final long nElems;

  public tech.v2.tensor.dimensions.ASTExampleClassRdr(java.lang.Object[], long[], long[], long[], long[]);
    Code:
       0: aload_0
       1: invokespecial #1                  // Method java/lang/Object."<init>":()V
       4: aload_0
       5: aload         5
       7: iconst_0
       8: laload
       9: putfield      #2                  // Field maxShapeStride0:J
      12: aload_0
      13: aload_2
      14: iconst_0
      15: laload
      16: putfield      #3                  // Field stride0:J
      19: aload_0
      20: aload_1
      21: iconst_1
      22: aaload
      23: checkcast     #4                  // class tech/v2/datatype/LongReader
      26: putfield      #5                  // Field shape1:Ltech/v2/datatype/LongReader;
      29: aload_0
      30: aload         4
      32: iconst_0
      33: laload
      34: aload         5
      36: iconst_0
      37: laload
      38: lmul
      39: putfield      #6                  // Field nElems:J
      42: return

  public long lsize();
    Code:
       0: aload_0
       1: getfield      #6                  // Field nElems:J
       4: lreturn

  public long read(long);
    Code:
       0: lload_1
       1: aload_0
       2: getfield      #2                  // Field maxShapeStride0:J
       5: ldiv
       6: aload_0
       7: getfield      #3                  // Field stride0:J
      10: lmul
      11: aload_0
      12: getfield      #5                  // Field shape1:Ltech/v2/datatype/LongReader;
      15: lload_1
      16: aload_0
      17: getfield      #5                  // Field shape1:Ltech/v2/datatype/LongReader;
      20: invokeinterface #7,  1            // InterfaceMethod tech/v2/datatype/LongReader.lsize:()J
      25: lrem
      26: invokeinterface #8,  3            // InterfaceMethod tech/v2/datatype/LongReader.read:(J)J
      31: ladd
      32: lreturn
}
