package tech.datatype;


public class ArrayViewBase
{
    public final int offset;
    public final int capacity;
    public ArrayViewBase(int off, int cap)
    {
	offset = off;
	capacity = cap;
    }
    public ArrayViewBase(int cap)
    {
	this(0,cap);
    }
    public final int length() { return capacity; }
    public final int index(int idx) { return offset + idx; }
    public final int checkIndex(int idx) throws Exception
    {
	int retval =index(idx);
	if ( retval >= capacity )
	    throw new Exception("Index past capacity.");
	if ( retval < 0 )
	    throw new Exception( "Index less than zero.");
	return retval;
    }
}
