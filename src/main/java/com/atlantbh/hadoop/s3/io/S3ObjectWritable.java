package com.atlantbh.hadoop.s3.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.amazonaws.services.s3.model.S3Object;

/**
 * Implementation of {@link WritableComparable} interface for {@link S3Object} class
 * 
 * @author seljaz
 *
 */
public class S3ObjectWritable extends S3Object implements
		WritableComparable<S3Object> {
	
	ByteBuffer inBuffer, outBuffer;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.setBucketName(Text.readString(in));
		this.setKey(Text.readString(in));

		// Metadata
		this.getObjectMetadata().setContentLength(in.readLong());
		this.getObjectMetadata().setLastModified(new Date(in.readLong()));
		this.getObjectMetadata().setCacheControl(Text.readString(in));
		this.getObjectMetadata().setContentDisposition(Text.readString(in));
		this.getObjectMetadata().setContentEncoding(Text.readString(in));
		this.getObjectMetadata().setContentMD5(Text.readString(in));
		this.getObjectMetadata().setContentType(Text.readString(in));
		this.getObjectMetadata().setServerSideEncryption(Text.readString(in));

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
		
		// Object metadata
		out.writeLong(getObjectMetadata().getContentLength());
		out.writeLong(getObjectMetadata().getLastModified().getTime());
		
		Text.writeString(out, getObjectMetadata().getCacheControl());
		Text.writeString(out, getObjectMetadata().getContentDisposition());
		Text.writeString(out, getObjectMetadata().getContentEncoding());
		Text.writeString(out, getObjectMetadata().getContentMD5());
		Text.writeString(out, getObjectMetadata().getContentType());
		Text.writeString(out, getObjectMetadata().getServerSideEncryption());

		// Version ID and eTag are read only properties so they're not serialized
		
		//TODO Add serialization for user and raw metadata
		
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
