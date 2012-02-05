package com.atlantbh.hadoop.s3.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3ObjectSummaryWritable extends S3ObjectSummary implements
		WritableComparable<S3ObjectSummary> {
	
	public S3ObjectSummaryWritable(){
		
	}
	
	public S3ObjectSummaryWritable(String bucketName, String key, String ETag, String storageClass, long size, Date lastModifiedDate, Owner owner) {
		setBucketName(bucketName);
		setKey(key);
		setETag(ETag);
		setStorageClass(storageClass);
		setSize(size);
		setLastModified(lastModifiedDate);
		setOwner(owner);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.setBucketName(Text.readString(in));
		this.setKey(Text.readString(in));
		this.setETag(Text.readString(in));
		this.setStorageClass(Text.readString(in));
		this.setSize(in.readLong());
		// LastModiifiedData and Owner are not serialized
		this.setLastModified(null);
		this.setOwner(null);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getBucketName());
		Text.writeString(out, getKey());
		Text.writeString(out, getETag());
		Text.writeString(out, getStorageClass());
		out.writeLong(getSize());
		// LastModiifiedData and Owner are not serialized
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
		return String.format("[bucketName=%s, key=%s, size=%l, ETag=%s, storageClass=%s]", getBucketName(), getKey(), getSize(), getETag(), getStorageClass());
	}
}
