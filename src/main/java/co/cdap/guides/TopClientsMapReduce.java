/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cdap.guides;

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.util.concurrent.TimeUnit;

/**
 * MapReduce job that computes topN clientIP in Apache access log.
 */
public class TopClientsMapReduce extends AbstractMapReduce {

  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("TopClientsMapReduce")
      .setDescription("MapReduce job that computes top 10 clients in the last 1 hour")
      .useOutputDataSet(LogAnalyticsApp.RESULTS_DATASET_NAME)
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

}
