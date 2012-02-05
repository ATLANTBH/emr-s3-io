package com.atlantbh.hadoop.s3.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.amazonaws.services.s3.model.S3Object;

public class S3ObjectWritable extends S3Object implements
		WritableComparable<S3Object> {
	
	ByteBuffer inBuffer, outBuffer;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.setBucketName(Text.readString(in));
		this.setKey(Text.readString(in));

		// LastModiifiedData and Owner are not serialized
		
		
		// Metadata
		this.getObjectMetadata().setContentLength(in.readLong());
		
		// Content
		int size = WritableUtils.readVInt(in);
		
		inBuffer = ByteBuffer.allocate(size);
		in.readFully(inBuffer.array());
		setObjectContent(new ByteArrayInputStream(inBuffer.array()));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getBucketName());
		Text.writeString(out, getKey());
		// LastModiifiedData and Owner are not serialized
		
		// metadata
		out.writeLong(getObjectMetadata().getContentLength());
		
		// Read bytes from into outBuffer
		outBuffer = ByteBuffer.allocate((int)getObjectMetadata().getContentLength());
		getObjectContent().read(outBuffer.array());
		//write size and content
		WritableUtils.writeVInt(out, outBuffer.capacity());
		out.write(outBuffer.array());
	}

	@Override
	public int compareTo(S3Object o) {
		if (getBucketName().compareTo(o.getBucketName()) == 0) {
			return getKey().compareTo(o.getKey());
		} else {
			return getBucketName().compareTo(o.getBucketName());
		}
	}
}
