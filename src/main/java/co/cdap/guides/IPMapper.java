package co.cdap.guides;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper that reads the Apache access log events and emits the IP as Key and count as Value.
 */
public class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private static final IntWritable OUTPUT_VALUE = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // The body of the stream event is contained in the Text value
    String streamBody = value.toString();
    if (streamBody != null  && !streamBody.isEmpty()) {
      String ip = streamBody.substring(0, streamBody.indexOf(" "));
      // Map output Key: IP and Value: Count
      context.write(new Text(ip), OUTPUT_VALUE);
    }
  }
}
