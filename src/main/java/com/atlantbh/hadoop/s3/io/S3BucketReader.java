package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

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
 * Class used for reading the objects from Amazon S3. Amazon S3 credentials are read from
 * "AwsCredentials.properties" file in jar
 * 
 * @author seljaz
 * 
 */
public class S3BucketReader {
	Logger LOG = LoggerFactory.getLogger(S3BucketReader.class);

	AmazonS3Client s3Client;

	ListObjectsRequest listObjectsRequest;
	ObjectListing objectListing;

	GetObjectRequest getObjectRequest;

	int currentPosition;
	S3ObjectSummary currentObject = null;
 
	/**
	 * Initializes S3 bucket reader for reading the keys from S3 bucket, having same prefix and staring with 
	 * key equals to marker
	 * @param bucketName S3 bucket name
	 * @param keyPrefix prefix of the keys
	 * @param marker first key to read from
	 * @param maxKeys maximal number of keys to retrieve from S3 in single call
	 * @throws IOException
	 */
	public S3BucketReader(String bucketName, String keyPrefix, String marker, int maxKeys) throws IOException {
		listObjectsRequest = new ListObjectsRequest(bucketName, keyPrefix, marker, "", maxKeys);
		s3Client = new AmazonS3Client(new PropertiesCredentials(
				S3RecordReader.class.getResourceAsStream("/AwsCredentials.properties")));
	}

	/**
	 * Reads next key from S3
	 * @return {@link S3ObjectSummary} of the key or <code>null</code> if there are no more keys to read
	 */
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
			// when we're at the end, check if there's more data to read
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

	/**
	 * Get S3 object using object summary
	 * 
	 * @param objectSummary {@link S3ObjectSummary} for key to retrieve
	 * @return {@link S3Object} read from S3
	 */
	public S3Object getObject(S3ObjectSummary objectSummary) {
		getObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey());
		return s3Client.getObject(getObjectRequest);
	}

	/**
	 * List S3 objects from bucket with same prefix
	 * 
	 * @param bucketName S3 bucket name
	 * @param keyPrefix prefix of the keys
	 * @param maxKeys maximal number of keys to return
	 * @return S3 object listing
	 */
	public ObjectListing listObjects(String bucketName, String keyPrefix, int maxKeys) {

		ListObjectsRequest request = new ListObjectsRequest(bucketName, keyPrefix, null, null, maxKeys);
		return s3Client.listObjects(request);
	}

	/**
	 * Get next batch of S3 objects
	 * 
	 * @param objectListing instance of {@link ObjectListing} used in previous listObject call 
	 * @return the next set of S3 keys
	 */
	public ObjectListing listObjects(ObjectListing objectListing) {

		return s3Client.listNextBatchOfObjects(objectListing);
	}
}