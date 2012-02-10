package com.atlantbh.hadoop.s3.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Implementation of {@link WritableComparable} interface for {@link S3ObjectSummary} class
 * 
 * @author seljaz
 *
 */
public class S3ObjectSummaryWritable extends S3ObjectSummary implements
		WritableComparable<S3ObjectSummary> {
	
	static Logger LOG = LoggerFactory.getLogger(S3InputSplit.class);
	
	public S3ObjectSummaryWritable(){
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.setBucketName(Text.readString(in));
		this.setKey(Text.readString(in));
		this.setETag(Text.readString(in));
		this.setStorageClass(Text.readString(in));
		
		// Set owner
		Owner owner = new Owner();
		owner.setId(Text.readString(in));
		owner.setDisplayName(Text.readString(in));
		
		this.setOwner(owner);
		this.setSize(in.readLong());
		this.setLastModified(new Date(in.readLong()));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getBucketName());
		Text.writeString(out, getKey());
		Text.writeString(out, getETag());
		Text.writeString(out, getStorageClass());
		Text.writeString(out, getOwner().getId());
		Text.writeString(out, getOwner().getDisplayName());
		out.writeLong(getSize());
		out.writeLong(getLastModified().getTime());
	}

	@Override
	public int compareTo(S3ObjectSummary o) {
		if (getBucketName().compareTo(o.getBucketName()) == 0) {
			return getKey().compareTo(o.getKey());
		} else {
			return getBucketName().compareTo(o.getBucketName());
		}
	}
	
	@Override
	public String toString() {
		return String.format("[bucketName=%s, key=%s, size=%d, ETag=%s, storageClass=%s, owner=%s, lastModified=%s]", getBucketName(), getKey(), getSize(), getETag(), getStorageClass(), getOwner(), getLastModified());
	}
}
