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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.atlantbh.hadoop.s3.io.S3RecordReader.S3BucketReader;

/**
 * Abstract S3 File Input format class
 * 
 * @author samir
 *
 * @param <K> Mapper key class
 * @param <V> Mapper value class
 */
public abstract class S3InputFormat<K, V> extends InputFormat<K, V>{
	
	Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);
	
	static String S3_BUCKET_NAME ="s3.bucket.name";
	static String S3_KEY_PREFIX ="s3.key.prefix";
	static String S3_NUM_OF_KEYS_PER_MAPPER ="s3.input.numOfKeys";
	static String S3_NUM_OF_MAPPERS ="s3.input.numOfMappers";
	
	S3BucketReader s3Reader;
	
	public S3InputFormat() throws IOException {
	}

	/*
	 * Returns list of DirectoryInputSplits containing all files specified using "abh.directory.path" property
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String bucketName = conf.get(S3_BUCKET_NAME);
		String keyPrefix = conf.get(S3_KEY_PREFIX);

		int numOfMappers = conf.getInt(S3_NUM_OF_MAPPERS, -1);
		int numOfKeysPerMapper = conf.getInt(S3_NUM_OF_KEYS_PER_MAPPER, -1);
		boolean useMappers = true;
		
		if (bucketName == null || "".equals(bucketName)) {
			throw new InvalidJobConfException("S3 bucket name cannot be empty");
		}
		
		if (numOfMappers == -1 && numOfKeysPerMapper == -1) {
			LOG.warn("Non of {} and {} properties are not set. Defaulting to numOfMappers=1 to determine input splits", S3_NUM_OF_KEYS_PER_MAPPER, S3_NUM_OF_MAPPERS);
			numOfMappers = 1;
			useMappers = true;
		} else if (numOfMappers > -1 && numOfKeysPerMapper > -1) {
			LOG.warn("Both {} and {} are set. Using numOfMappers value to determine input splits", S3_NUM_OF_KEYS_PER_MAPPER, S3_NUM_OF_MAPPERS);
			useMappers=true;
		} else if (numOfMappers == -1) {
			LOG.warn("Using {} value to determine input splits", S3_NUM_OF_KEYS_PER_MAPPER);
			useMappers = false;
		} else if (numOfKeysPerMapper == -1){
			LOG.warn("Using {} value to determine input splits", S3_NUM_OF_MAPPERS);
			useMappers = true;			
		}

		s3Reader = new S3BucketReader(bucketName, keyPrefix);

		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<S3ObjectSummary> objects = new ArrayList<S3ObjectSummary>();//s3Reader.listAllObjects(bucketName, keyPrefix);
		String marker = null;
		
		if (useMappers) {
			splits = new ArrayList<InputSplit>(numOfMappers);
		} else {
			int numOfKeys = s3Reader.countObjects(bucketName, keyPrefix);
			int numOfsplits = (int) Math.max(1, Math.round(numOfKeys/(double)numOfKeysPerMapper));
			splits = new ArrayList<InputSplit>(numOfsplits);
			
			for(int i=0; i<numOfsplits && !objects.isEmpty(); i++){
				int fromIndex = i*numOfKeysPerMapper;
				// index of last key in split so we can set marker for next input split
				int toIndex = (i+1)*numOfKeysPerMapper < objects.size() ? (i+1)*numOfKeysPerMapper : objects.size();
				
				List<S3ObjectSummary> records = objects.subList(fromIndex, toIndex);
				S3InputSplit split = new S3InputSplit();
				
				S3ObjectSummary lastObject = objects.get(toIndex-1);
				
				split.setBucketName(lastObject.getBucketName());
				split.setLastKey(lastObject.getKey());
				split.setKeyPrefix(keyPrefix);
				split.setMarker(marker);
				split.setSize(records.size());
				
				splits.add(split);
				
				// get marker
				marker = lastObject.getKey();
			}			
		}
		
		LOG.info("Number of input splits={}",splits.size());

		return splits;
	}
	
	/**
	 * Filters out keys we want to exclude
	 * 
	 * @param objects
	 * @param keyPattern
	 */
	@SuppressWarnings("unused")
	private void filterKeys(List<S3ObjectSummary> objects, String keyPattern) {
		for (S3ObjectSummary s3ObjectSummary : objects) {
			if (keyPattern.equalsIgnoreCase(s3ObjectSummary.getKey())) {
				objects.remove(s3ObjectSummary);
			}
		}
	}


	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = new Configuration(true);
		conf.set(S3_BUCKET_NAME, "hari_dev");
		conf.set(S3_KEY_PREFIX,"users");
		conf.setInt(S3_NUM_OF_KEYS_PER_MAPPER, 1000);
		
		S3InputFormat<Text, S3ObjectSummaryWritable> s3FileInput = new S3ObjectSummaryInputFormat();
		List<InputSplit> splits = s3FileInput.getSplits(new JobContext(conf, new JobID()));
		
		S3ObjectSummaryRecordReader reader = new S3ObjectSummaryRecordReader();
		
		for (InputSplit inputSplit : splits) {
			System.out.println(inputSplit.toString());
			reader.initialize(inputSplit, new TaskAttemptContext(conf, new TaskAttemptID()));
			
			int i=0;
			while (reader.nextKeyValue()) {
				System.out.printf("%d Key=%s. Value=%s\n", i++, reader.getCurrentKey(), reader.getCurrentValue());
			}
		}
	}	
}
