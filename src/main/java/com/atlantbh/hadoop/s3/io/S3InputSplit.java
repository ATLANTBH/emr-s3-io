package com.atlantbh.hadoop.s3.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Input split class
 * 
 * This class defines a subset of keys from Amazon S3 bucket. Following attributes define input split: bucket name, prefix, 
 * marker and last key. Combination of marker, prefix and last key is used to select only subset of keys from Amazon S3 
 * bucket. As keys in bucket are sorted alphabetically so defining marker (or start key) and last key (or end key) we can 
 * define interval of keys as input split. Key used as a marker is non-inclusive while last key is.
 * 
 * @author seljaz
 *
 */
public class S3InputSplit extends InputSplit implements Writable {
	
	static Logger LOG = LoggerFactory.getLogger(S3InputSplit.class);

	String bucketName;
	String keyPrefix;
	String marker;
	String lastKey;
	int size;
	
	public String getMarker() {
		return marker;
	}

	public void setMarker(String marker) {
		this.marker = marker;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	public String getLastKey() {
		return lastKey;
	}

	public void setLastKey(String lastKey) {
		this.lastKey = lastKey;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	S3InputSplit() {
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return size;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
			return new String[]{};
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getBucketName());
		Text.writeString(out, getKeyPrefix());
		Text.writeString(out, getMarker() != null ? getMarker() : "");
		Text.writeString(out, getLastKey());
		WritableUtils.writeVInt(out, getSize());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setBucketName(Text.readString(in));
		setKeyPrefix(Text.readString(in));
		setMarker(Text.readString(in));
		setLastKey(Text.readString(in));
		setSize(WritableUtils.readVInt(in));
	}
	
	@Override
	public String toString() {
		return String.format("[Bucket=%s, Prefix=%s, Marker=%s, LastKey=%s, Size=%d]", getBucketName(), getKeyPrefix(), getMarker(), getLastKey(), getSize());
	}
}
