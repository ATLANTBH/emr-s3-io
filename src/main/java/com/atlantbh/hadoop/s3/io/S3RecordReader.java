package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Abstract S3 record reader class
 * 
 * @author seljaz
 * @param <T>
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

	/**
	 * 
	 * @author seljaz
	 *
	 */
	protected static class S3BucketReader {
		Logger LOG = LoggerFactory.getLogger(S3BucketReader.class);

		AmazonS3Client s3Client;

		ListObjectsRequest listObjectsRequest;
		ObjectListing objectListing;

		GetObjectRequest getObjectRequest;

		int currentPosition;
		S3ObjectSummary currentObject = null;

		public S3BucketReader(String bucketName, String keyPrefix, String marker, int maxKeys) throws IOException {
			listObjectsRequest = new ListObjectsRequest(bucketName, keyPrefix, marker, "", maxKeys);
			s3Client = new AmazonS3Client(new PropertiesCredentials(
					S3RecordReader.class.getResourceAsStream("/AwsCredentials.properties")));
		}

		public S3ObjectSummary getNextKey() {
			if (objectListing == null) {
				LOG.debug("Listing objects");
				objectListing = s3Client.listObjects(listObjectsRequest);
			}

			// we have more elements in list
			if (currentPosition < objectListing.getObjectSummaries().size()) {
				currentObject = objectListing.getObjectSummaries().get(currentPosition++);
			} else if (objectListing.isTruncated()) {
				LOG.debug("Current batch reached its end. Fetching next page.");
				// when we're at the end, check if thers more data
				listObjectsRequest.setMarker(objectListing.getNextMarker());
				LOG.debug("New marker is set to {}", listObjectsRequest.getMarker());

				objectListing = s3Client.listObjects(listObjectsRequest);
				currentPosition = 0;

				if (currentPosition < objectListing.getObjectSummaries().size()) {
					currentObject = objectListing.getObjectSummaries().get(currentPosition++);
				} else {
					LOG.debug("No more objects to read");
					currentObject = null;
				}
			} else {
				currentObject = null;
			}

			return currentObject;
		}

		public S3Object getObject(S3ObjectSummary objectSummary) {
			getObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey());
			return s3Client.getObject(getObjectRequest);
		}

		/**
		 * 
		 * @param bucketName
		 * @param keyPrefix
		 * @param maxKeys
		 * @param numOfKeys
		 * @return
		 */
		public ObjectListing listObjects(String bucketName, String keyPrefix, int maxKeys) {

			ListObjectsRequest request = new ListObjectsRequest(bucketName, keyPrefix, null, null, maxKeys);
			return s3Client.listObjects(request);
		}
		
		/**
		 * 
		 * @param objectListing
		 * @return
		 */
		public ObjectListing listObjects(ObjectListing objectListing) {

			return s3Client.listNextBatchOfObjects(objectListing);
		}
	}
}