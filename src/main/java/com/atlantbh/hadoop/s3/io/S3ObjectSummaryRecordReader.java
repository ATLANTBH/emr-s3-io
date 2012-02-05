package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class S3ObjectSummaryRecordReader extends S3RecordReader<Text, S3ObjectSummaryWritable> {
	
	public S3ObjectSummaryRecordReader() {
		outKey = new Text();
		outValue = new S3ObjectSummaryWritable();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// we have read another record
		if (super.nextKeyValue()) {
			// set out key
			outKey.set(String.format("%s/%s", currentKey.getBucketName(), currentKey.getKey()));
			
			// set out value
			outValue.setBucketName(currentKey.getBucketName());
			outValue.setKey(currentKey.getKey());
			outValue.setETag(currentKey.getETag());
			outValue.setLastModified(currentKey.getLastModified());
			outValue.setOwner(currentKey.getOwner());
			outValue.setSize(currentKey.getSize());
			outValue.setStorageClass(currentKey.getStorageClass());
			
			return true;
		} else {
			return false;
		}
	}
}
