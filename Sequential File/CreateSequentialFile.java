package com.sequential;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateSequentialFile extends Configured implements Tool {

@Override
public int run(String[] args) throws Exception {

if (args.length != 2) {
System.out.printf("Usage: CreateSequenceFile <input dir> <output dir>\n");
return -1;
}

Job job = new Job(getConf());
job.setJarByClass(CreateSequentialFile.class);
job.setJobName("Create Sequence File");

/*
* TODO implement
*/

boolean success = job.waitForCompletion(true);
return success ? 0 : 1;
}

public static void main(String[] args) throws Exception {
int exitCode = ToolRunner.run(new Configuration(), new CreateSequentialFile(), args);
System.exit(exitCode);
}
}

