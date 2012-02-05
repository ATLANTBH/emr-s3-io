package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import com.amazonaws.services.s3.model.S3Object;

public class S3ObjectRecordReader extends S3RecordReader<S3ObjectSummaryWritable, S3ObjectWritable> {
	S3Object object;
	
	public S3ObjectRecordReader() {
		outValue = new S3ObjectWritable();
		outKey = new S3ObjectSummaryWritable();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// we have read another record
		if (super.nextKeyValue()) {
			object = reader.getObject(currentKey);
			
			// poupulate key
			outKey.setBucketName(currentKey.getBucketName());
			outKey.setETag(currentKey.getETag());
			outKey.setKey(currentKey.getKey());
			outKey.setLastModified(currentKey.getLastModified());
			outKey.setOwner(currentKey.getOwner());
			outKey.setSize(currentKey.getSize());
			outKey.setStorageClass(currentKey.getStorageClass());
			
			//populate value
			outValue.setBucketName(object.getBucketName());
			outValue.setKey(object.getKey());
			outValue.setObjectContent(object.getObjectContent());
			outValue.setObjectMetadata(object.getObjectMetadata());

			return true;
		} else {
			return false;
		}
	}
}
