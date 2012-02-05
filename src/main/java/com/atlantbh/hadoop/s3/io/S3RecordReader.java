package com.atlantbh.hadoop.s3.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * Reads <IntWritable, Path> from DirectoryInoutSplit
 * 
 * @author seljaz
 * @param <T>
 * 
 */
public abstract class S3RecordReader<KEY, VALUE> extends
		RecordReader<KEY, VALUE> {

	/**
	 * Number of files to get from S3 in single request. Default value is 100
	 */
	static String S3_MAX_KEYS = "s3.input.format.max.keys";

	Logger LOG = LoggerFactory.getLogger(S3RecordReader.class);

	String bucketName;
	String keyPrefix;
	String marker;
	String lastKey;
	int maxKeys;

	S3BucketReader reader = null;

	S3ObjectSummary currentKey;

	KEY outKey = null;
	VALUE outValue = null;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		S3InputSplit inputSplit = (S3InputSplit) split;

		bucketName = inputSplit.getBucketName();
		keyPrefix = inputSplit.getKeyPrefix();
		marker = inputSplit.getMarker();
		lastKey = inputSplit.lastKey;
		maxKeys = context.getConfiguration().getInt(S3_MAX_KEYS, 100);

		reader = new S3BucketReader(bucketName, keyPrefix, marker, maxKeys);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		while ((currentKey = reader.getNextKey()) != null) {
			// we have reached end of the split
			if (currentKey.getKey().compareTo(lastKey)>0) {
				return false;
			} else {
				return true;
			}
		}
		
		return false;
	}

	@Override
	public KEY getCurrentKey() throws IOException,
			InterruptedException {
		return outKey;
	}

	@Override
	public VALUE getCurrentValue() throws IOException, InterruptedException {
		return outValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}

	protected static class S3BucketReader {
		Logger LOG = LoggerFactory.getLogger(S3BucketReader.class);

		AmazonS3Client s3Client;
		
		ListObjectsRequest listObjectsRequest;
		ObjectListing objectListing;
		
		GetObjectRequest getObjectRequest;

		int currentPosition;
		S3ObjectSummary currentObject = null;

		public S3BucketReader(String bucketName, String keyPrefix,
				String marker, int maxKeys) throws IOException {
			listObjectsRequest = new ListObjectsRequest(bucketName, keyPrefix,
					marker, "", maxKeys);
			s3Client = new AmazonS3Client(new PropertiesCredentials(
					S3Utils.class
							.getResourceAsStream("/AwsCredentials.properties")));
		}
		
		public S3BucketReader(String bucketName, String keyPrefix) throws IOException {
			this(bucketName, keyPrefix, "", 500);
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
		
		public S3Object getObject(S3ObjectSummary objectSummary){
			getObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey());
			return s3Client.getObject(getObjectRequest);
		}
		
		/**
		 * Retrieves list of object summaries from S3
		 * 
		 * @param bucketName
		 * @param keyPrefix
		 * @param listing
		 * @param objectListing
		 * @return true if we have more objects in S3
		 */
		public boolean listObjects(String bucketName, String keyPrefix, ObjectListing listing, List<S3ObjectSummary> objectListing) {
			
			// are we calling S3 for the first time
			if (listing == null) {
				listing = s3Client.listObjects(bucketName, keyPrefix);
			} else {
				listing = s3Client.listNextBatchOfObjects(listing);
			}
			
			// re-init results list
			if (objectListing == null) {
				objectListing = new ArrayList<S3ObjectSummary>(listing.getObjectSummaries().size());
			} else {
				objectListing.clear();
			}
			
			objectListing.addAll(listing.getObjectSummaries());

			// do we have more files or not
			if (listing.isTruncated()) {
				return true;
			} else {
				return false;
			}
		}
		
		/**
		 * Gets the number of keys
		 * 
		 * @param bucketName
		 * @param keyPrefix
		 * @return number of keys
		 */
		public int countObjects(String bucketName, String keyPrefix) {
			int count = 0;
			
			ObjectListing listing = s3Client.listObjects(bucketName, keyPrefix);
			
			do {
				count += listing.getObjectSummaries().size();
			} while (listing.isTruncated());
			
			return count;
		}
		
		
		/**
		 * Get all object summaries from S3
		 * @param bucketName
		 * @param keyPrefix
		 * @return 
		 */
		public List<S3ObjectSummary> listAllObjects(String bucketName, String keyPrefix) {
			
			List<S3ObjectSummary> objects = new ArrayList<S3ObjectSummary>();
			ObjectListing listing = s3Client.listObjects(bucketName, keyPrefix);
			
			do {
				objects.addAll(listing.getObjectSummaries());
			} while (listing.isTruncated());
			
			return objects;
		}

		public static void main(String[] args) {
			try {
				S3BucketReader reader = new S3BucketReader("hari_dev", "users/10/", "", 3);
				S3ObjectSummary object;
				
				int i=0;
				while ((object = reader.getNextKey()) != null) {
					System.out.println(i++ + object.getBucketName() + "-"+ object.getKey());
				}
				
				System.out.println(reader.listAllObjects("hari_dev", "users/10/").size());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}