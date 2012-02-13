package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Abstract S3 record reader class
 * 
 * @author seljaz
 * @param <KEY>
 * @param <VALUE>
 *  
 */
public abstract class S3RecordReader<KEY, VALUE> extends RecordReader<KEY, VALUE> {

	Logger LOG = LoggerFactory.getLogger(S3RecordReader.class);

	String bucketName;
	String keyPrefix;
	String marker;
	String lastKey;
	
	int size;
	int currentPosition = 0;
	int maxKeys;

	S3BucketReader reader = null;

	S3ObjectSummary currentKey;

	KEY outKey = null;
	VALUE outValue = null;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		S3InputSplit inputSplit = (S3InputSplit) split;

		bucketName = inputSplit.getBucketName();
		keyPrefix = inputSplit.getKeyPrefix();
		marker = inputSplit.getMarker();
		lastKey = inputSplit.lastKey;
		
		maxKeys = context.getConfiguration().getInt(S3InputFormat.S3_MAX_KEYS, 100);

		reader = new S3BucketReader(bucketName, keyPrefix, marker, maxKeys);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while ((currentKey = reader.getNextKey()) != null) {
			// have we reached end of the split
			if (currentKey.getKey().compareTo(lastKey) <= 0) {
				currentPosition++;
				return true;
			} else {
				return false;
			}
		}

		return false;
	}

	@Override
	public KEY getCurrentKey() throws IOException, InterruptedException {
		return outKey;
	}

	@Override
	public VALUE getCurrentValue() throws IOException, InterruptedException {
		return outValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return currentPosition/(float)size;
	}

	@Override
	public void close() throws IOException {
	}
}