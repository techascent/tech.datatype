package think.datatype;

import java.util.Arrays;

public final class IntArrayView extends ArrayViewBase
{
    public final int[] data;
    void checkDataLength() throws Exception
    {
	if ( data.length < (offset + capacity) )
	    throw new Exception
		(String.format("data length %s is less than offset %s + capacity %s.",
			       data.length, offset, capacity));
    }
    public IntArrayView( int[] d, int o, int cap ) throws Exception
    {
	super(o, cap);
	data = d;
	checkDataLength();
    }
    public IntArrayView( int[] d, int o )
    {
	super(o, d.length - o);
	data = d;
    }
    public IntArrayView( int[] d )
    {
	super(d.length);
	data = d;
    }

    /**
       Member function construction to allow chaining from an existing view while preserving type.
     */
    public final IntArrayView construct( int offset, int capacity ) throws Exception
    {
	return new IntArrayView(data, offset, capacity);
    }

    public final int get(int idx)
    {
	return data[index(idx)];
    }
    public final void set(int idx, int value)
    {
	data[index(idx)] = value;
    }
    public final void pluseq(int idx, int value)
    {
	data[index(idx)] += value;
    }
    public final void minuseq(int idx, int value)
    {
	data[index(idx)] -= value;
    }
    public final void multeq(int idx, int value)
    {
	data[index(idx)] *= value;
    }
    public final void diveq(int idx, int value)
    {
	data[index(idx)] /= value;
    }
    public final void fill(int value)
    {	
        Arrays.fill(data, offset, (offset + capacity), value);
    }
    public final IntArrayView toView(int new_offset, int len) throws Exception
    {
	return new IntArrayView(data, offset + new_offset, len);
    }
    public final IntArrayView toView(int offset) throws Exception
    {
	return toView(offset, length() - offset);
    }
}
