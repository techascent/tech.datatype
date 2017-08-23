package think.datatype;

import java.util.Arrays;

public final class ShortArrayView extends ArrayViewBase
{
    public final short[] data;
    void checkDataLength() throws Exception
    {
	if ( data.length < (offset + capacity) )
	    throw new Exception
		(String.format("data length %s is less than offset %s + capacity %s.",
			       data.length, offset, capacity));
    }
    public ShortArrayView( short[] d, int o, int cap ) throws Exception
    {
	super(o, cap);
	data = d;
	checkDataLength();
    }
    public ShortArrayView( short[] d, int o )
    {
	super(o, d.length - o);
	data = d;
    }
    public ShortArrayView( short[] d )
    {
	super(d.length);
	data = d;
    }

    /**
       Member function construction to allow chaining from an existing view while preserving type.
     */
    public final ShortArrayView construct( int offset, int capacity ) throws Exception
    {
	return new ShortArrayView(data, offset, capacity);
    }

    public final short get(int idx)
    {
	return data[index(idx)];
    }
    public final void set(int idx, short value)
    {
	data[index(idx)] = value;
    }
    public final void pluseq(int idx, short value)
    {
	data[index(idx)] += value;
    }
    public final void minuseq(int idx, short value)
    {
	data[index(idx)] -= value;
    }
    public final void multeq(int idx, short value)
    {
	data[index(idx)] *= value;
    }
    public final void diveq(int idx, short value)
    {
	data[index(idx)] /= value;
    }
    public final void fill(short value)
    {	
        Arrays.fill(data, offset, (offset + capacity), value);
    }
    public final ShortArrayView toView(int new_offset, int len) throws Exception
    {
	return new ShortArrayView(data, offset + new_offset, len);
    }
    public final ShortArrayView toView(int offset) throws Exception
    {
	return toView(offset, length() - offset);
    }
}
