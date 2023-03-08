package com.cache;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), WordCount.class); 
    conf.setJobName("wordcount");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    List<String> other_args = new ArrayList<String>(); 

    for (int i=0; i < args.length; ++i) {
      if ("-skip".equals(args[i])) {
        DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf); 
        conf.setBoolean("wordcount.skip.patterns", true); 
      } else {

        other_args.add(args[i]);
      }
    }
    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception { 

    int res = ToolRunner.run(new Configuration(), new WordCount(), args); 
    System.exit(res);
  }
} 

public class CacheDriver {

}
