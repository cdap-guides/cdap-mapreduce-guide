package co.cdap.guides;

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * MapReduce job that computes topN clientIP in Apache access log.
 */
public class TopClientsMapReduce extends AbstractMapReduce {

  private static final int COUNT = 10;

  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("TopClientsMapReduce")
      .setDescription("MapReduce job that computes top 10 clients in the last 1 hour")
      .useOutputDataSet(LogAnalyticsApp.DATASET_NAME)
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {

    // Get the Hadoop job context, set Mapper, reducer and combiner.
    Job job = (Job) context.getHadoopJob();

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setMapperClass(IPMapper.class);

    job.setCombinerClass(CountsCombiner.class);

    // Number of reducer set to 1 to compute topN in a single reducer.
    job.setNumReduceTasks(1);
    job.setReducerClass(TopNClientsReducer.class);

    // Read events from last 60 minutes as input to the mapper.
    final long endTime = context.getLogicalStartTime();
    final long startTime = endTime - TimeUnit.MINUTES.toMillis(60);
    StreamBatchReadable.useStreamInput(context, "logEvent", startTime, endTime);
  }

  /**
   * Mapper that reads the Apache access log events and emits the IP and count.
   */
  public static class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

  /**
   * Reducer to compute topN clients by Count.
   */
  public static class TopNClientsReducer extends Reducer<Text, IntWritable, byte[], List<ClientCount>> {
    private static final PriorityQueue<ClientCount> priorityQueue = new PriorityQueue<ClientCount>(COUNT);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                          throws IOException, InterruptedException {
      // For each Key: IP, aggregate the Value: Count.
      int count = 0;
      for (IntWritable data : values) {
        count += data.get();
      }
      // Store the Key and Value in a priority queue.
      priorityQueue.add(new ClientCount(key.toString(), count));

      // Ensure the priority queue is bounded.
      if (priorityQueue.size() > COUNT) {
        priorityQueue.poll();
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Write topN results in reduce output. Since the "topN" (ObjectStore) Dataset is used as output the entries
      // will be written to the Dataset without any additional effort.
      List<ClientCount> topNResults = Lists.newArrayList();
      while (priorityQueue.size() != 0) {
        topNResults.add(priorityQueue.poll());
      }
      context.write(LogAnalyticsApp.DATASET_RESULTS_KEY, topNResults);
    }
  }

  /**
   * Combiner to aggregate Map output locally when each mapper runs. This is a simple aggregator.
   */
  private static class CountsCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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
}
