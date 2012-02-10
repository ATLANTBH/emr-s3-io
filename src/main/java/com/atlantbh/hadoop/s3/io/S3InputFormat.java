package com.atlantbh.hadoop.s3.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.atlantbh.hadoop.s3.io.S3RecordReader.S3BucketReader;

/**
 * Abstract S3 File Input format class
 * 
 * @author samir
 * 
 * @param <K>
 *            Mapper key class
 * @param <V>
 *            Mapper value class
 */
public abstract class S3InputFormat<K, V> extends InputFormat<K, V> {

	Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);

	static String S3_BUCKET_NAME = "s3.bucket.name";
	/**
	 * Number of files to get from S3 in single request. Default value is 100
	 */
	static String S3_KEY_PREFIX = "s3.key.prefix";
	static String S3_MAX_KEYS = "s3.max.keys";
	
	static String S3_NUM_OF_KEYS_PER_MAPPER = "s3.input.numOfKeys";
	static String S3_NUM_OF_MAPPERS = "s3.input.numOfMappers";

	S3BucketReader s3Reader;

	public S3InputFormat() throws IOException {
	}

	/**
	 * Returns list of {@link S3InputSplit} 
	 * 
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String bucketName = conf.get(S3_BUCKET_NAME);
		String keyPrefix = conf.get(S3_KEY_PREFIX);
		
		int maxKeys = conf.getInt(S3_MAX_KEYS, 500);

		int numOfMappers = conf.getInt(S3_NUM_OF_MAPPERS, -1);
		int numOfKeysPerMapper = conf.getInt(S3_NUM_OF_KEYS_PER_MAPPER, -1);
		boolean useMappers = true;

		if (bucketName == null || "".equals(bucketName)) {
			throw new InvalidJobConfException("S3 bucket name cannot be empty");
		}

		if (numOfMappers == -1 && numOfKeysPerMapper == -1) {
			LOG.warn("Non of {} and {} properties are not set. Defaulting to numOfMappers=1 to determine input splits",
					S3_NUM_OF_KEYS_PER_MAPPER, S3_NUM_OF_MAPPERS);
			numOfMappers = 1;
			useMappers = true;
		} else if (numOfMappers > -1 && numOfKeysPerMapper > -1) {
			LOG.warn("Both {} and {} are set. Using numOfMappers value to determine input splits",
					S3_NUM_OF_KEYS_PER_MAPPER, S3_NUM_OF_MAPPERS);
			useMappers = true;
		} else if (numOfMappers == -1) {
			LOG.warn("Using {} value to determine input splits", S3_NUM_OF_KEYS_PER_MAPPER);
			useMappers = false;
		} else if (numOfKeysPerMapper == -1) {
			LOG.warn("Using {} value to determine input splits", S3_NUM_OF_MAPPERS);
			useMappers = true;
		}

		s3Reader = new S3BucketReader(bucketName, keyPrefix, null, maxKeys);

		List<InputSplit> splits = new ArrayList<InputSplit>();

		if (useMappers) {
			// not implemented
			throw new IOException("Defining S3InputFormat with number of mappers is not implemented");
		} else {
			S3ObjectSummary startKey = null;
			S3ObjectSummary endKey = null;

			int batchSize = 0;
			
			int numOfSplits = 0;
			int numOfCalls = 0;

			int maxKeyIDX = 0;
			int currentKeyIDX = 0;
			int nextKeyIDX = 0;

			ObjectListing listing = null;
			boolean isLastCall = true;

			// split all keys starting with "keyPrefix" into splits of
			// "numOfKeysPerMapper" keys
			do {
				// for first time we have to build request after that use
				// previous listing to get next batch
				if (listing == null) {
					listing = s3Reader.listObjects(bucketName, keyPrefix, maxKeys);
				} else {
					listing = s3Reader.listObjects(listing);
				}

				// Is this last call to WS (last batch of objects)
				isLastCall = !listing.isTruncated();

				// Size of the batch from last WS call
				batchSize = listing.getObjectSummaries().size();

				// Absolute index of last key from batch
				maxKeyIDX = numOfCalls * maxKeys + batchSize;

				// Absolute indexes of current and next keys
				currentKeyIDX = numOfSplits * numOfKeysPerMapper;
				// if there are no more keys to process, index of last key is selected
				nextKeyIDX = (numOfSplits + 1) * numOfKeysPerMapper > maxKeyIDX && isLastCall ? maxKeyIDX : (numOfSplits + 1) * numOfKeysPerMapper;

				// create one input split for each key which is in current range
				while (nextKeyIDX <= maxKeyIDX) {

					startKey = endKey;
					endKey = listing.getObjectSummaries().get((nextKeyIDX - 1) % maxKeys);

					// Create new input split
					S3InputSplit split = new S3InputSplit();
					split.setBucketName(bucketName);
					split.setKeyPrefix(keyPrefix);
					split.setMarker(startKey != null ? startKey.getKey() : null);
					split.setLastKey(endKey.getKey());
					split.setSize(nextKeyIDX - currentKeyIDX);

					splits.add(split);
					numOfSplits++;

					// Stop when last key was processed
					if (nextKeyIDX == maxKeyIDX && isLastCall) {
						break;
					}
					
					// Set next key index
					currentKeyIDX = numOfSplits * numOfKeysPerMapper;
					nextKeyIDX = (numOfSplits + 1) * numOfKeysPerMapper > maxKeyIDX && isLastCall ? maxKeyIDX : (numOfSplits + 1) * numOfKeysPerMapper;
				}
				numOfCalls++;
			} while (!isLastCall);
		}

		LOG.info("Number of input splits={}", splits.size());

		return splits;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = new Configuration(true);
		conf.set(S3_BUCKET_NAME, "hari_dev");
		conf.set(S3_KEY_PREFIX, "users/299/avatar_f");
		conf.setInt(S3_NUM_OF_KEYS_PER_MAPPER, 1);

		S3InputFormat<Text, S3ObjectSummaryWritable> s3FileInput = new S3ObjectSummaryInputFormat();
		List<InputSplit> splits = s3FileInput.getSplits(new JobContext(conf, new JobID()));

		S3ObjectSummaryRecordReader reader = new S3ObjectSummaryRecordReader();

		for (InputSplit inputSplit : splits) {
			System.out.println(inputSplit.toString());
			reader.initialize(inputSplit, new TaskAttemptContext(conf, new TaskAttemptID()));

			int i = 0;
			while (reader.nextKeyValue()) {
				System.out.printf("\t%d Key=%s. Value=%s\n", i++, reader.getCurrentKey(), reader.getCurrentValue());
			}
		}
	}
}
