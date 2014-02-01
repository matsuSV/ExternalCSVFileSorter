package com.csvfile.sorter.samples.serialize;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * シリアライズのパフォーマンス改善
 *
 */
public class RowData implements Externalizable {

	private List<String> data = null;
	
	public RowData( List<String> data ) {
		this.data = data;
	}
	
	@Override
	public void writeExternal( ObjectOutput out) throws IOException {
		
		out.writeInt(data.size());
		
		for( String value : data ) {
			if( null != value ) {
				out.writeBoolean(true);
				out.writeUTF(value);
			} else {
				out.writeBoolean(false);                 
			}
		}
	}
	
	@Override
	public void readExternal( ObjectInput in) throws IOException, ClassNotFoundException {
		
		int columnCount = in.readInt();
		
		data = new ArrayList<String>( columnCount );
		
		for( int i = 0; i < columnCount; i++) {
			if( in.readBoolean() ) {
				data.add(in.readUTF());
			} else {
				data.add(null);
			}
		}
	}
	
	public List<String> getData() {
		return this.data;
	}
}