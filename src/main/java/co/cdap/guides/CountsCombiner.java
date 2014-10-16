package co.cdap.guides;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner to aggregate Map output locally when each mapper runs. This is a simple aggregator.
 */
class CountsCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                        throws IOException, InterruptedException {

    int count = 0;
    for (IntWritable val : values) {
      count += val.get();
    }
    context.write(key, new IntWritable(count));
  }
}
