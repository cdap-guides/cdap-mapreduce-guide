package co.cdap.guides;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Reducer to compute topN clients by Count.
 */
public class TopNClientsReducer extends Reducer<Text, IntWritable, byte[], List<ClientCount>> {

  private static final int COUNT = 10;
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

    // Ensure the priority queue is always contains topN count.
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
    context.write(TopClientsService.DATASET_RESULTS_KEY, topNResults);
  }
}
