import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeStats {
  // Stats intended to help compute average
  public static int count = 0;
  public static int totsum = 0;

  public static class CrimeRowMapper extends Mapper<Object,CrimeRow,Text,IntWritable> {
    public void map(Object key, CrimeRow value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(value.offense), new IntWritable(1));
    };
  };

  public static class CrimeRowReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      int sum = 0;
      for(IntWritable i : values) {
        sum += i.get();
      };
      // Output the per-crime count statistic
      context.write(key, new IntWritable(sum));

      // Keep (global) count of the number of distinct crimes
      count += 1;

      // Keep running sum of total incidents
      totsum += sum;

      // Iteratively determine the global Average (incidents)/(crime type)
      context.write(new Text("Average"), new IntWritable(totsum/count));
    };
  };

  public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Crime Stats");
    job.setJarByClass(CrimeStats.class);
    job.setMapperClass(CrimeRowMapper.class);
    job.setCombinerClass(CrimeRowReducer.class);
    job.setReducerClass(CrimeRowReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setInputFormatClass(CrimeRecordInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  };
};
