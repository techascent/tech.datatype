Compiled from "ASTExampleClass.java"
public class tech.v2.tensor.dimensions.ASTExampleClass {
  public final long maxShapeStride0;

  public final long stride0;

  public final long shape1;

  public final long nElems;

  public tech.v2.tensor.dimensions.ASTExampleClass(java.lang.Object[], long[], long[], long[], long[]);
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
      23: checkcast     #4                  // class java/lang/Long
      26: invokevirtual #5                  // Method java/lang/Long.longValue:()J
      29: putfield      #6                  // Field shape1:J
      32: aload_0
      33: aload         4
      35: iconst_0
      36: laload
      37: aload         5
      39: iconst_0
      40: laload
      41: lmul
      42: putfield      #7                  // Field nElems:J
      45: return

  public long lsize();
    Code:
       0: aload_0
       1: getfield      #7                  // Field nElems:J
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
      11: lload_1
      12: aload_0
      13: getfield      #6                  // Field shape1:J
      16: lrem
      17: ladd
      18: lreturn
}
