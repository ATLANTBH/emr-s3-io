package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class S3ObjectInputFormat extends S3InputFormat<S3ObjectSummaryWritable, S3ObjectWritable>  {

	public S3ObjectInputFormat() throws IOException {
		super();
	}

	@Override
	public RecordReader<S3ObjectSummaryWritable, S3ObjectWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new S3ObjectRecordReader();
	}
}
