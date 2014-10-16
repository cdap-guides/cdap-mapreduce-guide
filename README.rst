Batch Data Processing With CDAP
===============================

`MapReduce <http://research.google.com/archive/mapreduce.html>`_ is the most popular paradigm for processing large
amounts of data in a reliable and fault-tolerant manner. In this guide you will learn how to batch process data using
MapReduce in the `Cask Data Application Platform (CDAP). <http://cdap.io>`_

What You Will Build
-------------------

This guide will take you through building a CDAP application that uses ingested Apache access log events to compute
top 10 client IPs in a specific time-range and query the results. You will:

* Build `MapReduce <http://docs.cask.co/cdap/current/en/dev-guide.html#mapreduce>`_ job to process Apache access log events
* Use `Dataset <http://docs.cask.co/cdap/current/en/dev-guide.html#datasets>`_ to persist results of the MapReduce job
* Build a `Service <http://docs.cask.co/cdap/current/en/dev-guide.html#services>`_ to serve the results via HTTP


What You Will Need
------------------

* `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_
* `Apache Maven 3.0+ <http://maven.apache.org/>`_
* `CDAP SDK <http://docs.cdap.io/cdap/current/en/getstarted.html#download-and-setup>`_

Let’s Build It!
---------------

Following sections will guide you through building an application from scratch. If you are interested in deploying and
running the application right away, you can clone its source code from this github repository. In that case feel free
to skip the next two sections and jump right to Build & Run section.

Application Design
------------------

The application will assume that the Apache access logs are ingested into a Stream. The log events can be ingested
into a Stream continuously in real-time or in batches, which doesn’t affect the ability to consume it by MapReduce.

MapReduce job extracts necessary information from raw logs and computes top 10 Client IPs by traffic in a specific time range.
The results of the computation are persisted in a Dataset.

The application also contains a Service that exposes HTTP endpoint to access data stored in a Dataset.

|(AppDesign)|


Implementation
--------------

The first step is to get our application structure set up.  We will use a standard Maven project structure for all of
the source code files::

  ./pom.xml
  ./src/main/java/co/cask/cdap/guides/ClientCount.java
  ./src/main/java/co/cask/cdap/guides/LogAnalyticsApp.java
  ./src/main/java/co/cask/cdap/guides/package-info.java
  ./src/main/java/co/cask/cdap/guides/TopClientsMapReduce.java
  ./src/main/java/co/cask/cdap/guides/IPMapper.java
  ./src/main/java/co/cask/cdap/guides/TopNClientsReducer.java
  ./src/main/java/co/cask/cdap/guides/CountsCombiner.java
  ./src/main/java/co/cask/cdap/guides/TopClientsService.java
  ./src/test/java/co/cdap/guides/LogAnalyticsAppTest.java


The CDAP application is identified by LogAnalyticsApp class. This class extends an
`AbstractApplication <http://docs.cdap.io/cdap/2.5.1/en/javadocs/co/cask/cdap/api/app/AbstractApplication.html>`_,
and overrides the ``configure()`` method in order to define all of the application components:

.. code:: java

  public class LogAnalyticsApp extends AbstractApplication {
    
    public static final String DATASET_NAME = "topClients";

    @Override
    public void configure() {
      setName("LogAnalyticsApp");
      setDescription("An application that computes top 10 clientIPs from Apache access log data");
      addStream(new Stream("logEvents"));
      addMapReduce(new TopClientsMapReduce());
      try {
        DatasetProperties props = ObjectStores.objectStoreProperties(Types.listOf(ClientCount.class),
                                                                     DatasetProperties.EMPTY);
        createDataset(DATASET_NAME, ObjectStore.class, props);
      } catch (UnsupportedTypeException e) {
        throw Throwables.propagate(e);
      }
      addService(new TopClientsService());
    }
  }

The LogAnalytics application defines a new `Stream <http://docs.cdap.io/cdap/current/en/dev-guide.html#streams>`_
where Apache access logs are ingested to.

The log events can be ingested into CDAP stream. Once the data is ingested into the stream the events
can be processed in real-time or batch. In our application, we will process the events in batch using the
TopClientsMapReduce program and compute top10 client IPs in a specific time-range.

The results of the MapReduce job is persisted into a Dataset, the application uses createDataset method to define
the Dataset. Finally, the application adds a service to query the results from the Dataset.

Let's take a closer look at the MapReduce program.

The TopClientsMapReduce job extends an 
`AbstractMapReduce <http://docs.cdap.io/cdap/2.5.1/en/javadocs/co/cask/cdap/api/mapreduce/AbstractMapReduce.html>`_
class and overrides the ``configure()`` and ``beforeSubmit()`` methods.

* ``configure()`` method configures a MapReduce job, by setting job name, description and output Dataset.

* ``beforeSubmit()`` method is invoked at runtime, before the MapReduce job is executed. Here, you can access the
Hadoop job configuration through the MapReduceContext. Mapper and Reducer classes as well as the intermediate data
format are set in this method.

.. code:: java

  public class TopClientsMapReduce extends AbstractMapReduce {

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
      StreamBatchReadable.useStreamInput(context, "logEvents", startTime, endTime);
    }
  }


In this example Mapper and Reducer classes are built by implementing
`Hadoop APIs <http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/package-summary.html>`_

In the application, the Mapper class reads the Apache access log event from the stream and produces clientIP and count
as the intermediate map output key and value.

.. code:: java

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

The reducer class gets the clientIP and count from the map jobs and then aggregates the count for each cilentIP and
stores it in a priority queue. The number of reducer is set to 1, so that all the results go into the same reducer
to compute top 10 results. The top 10 results are written to the MapReduce context in the cleanup method of the
Reducer, which is called once during the end of the task. Writing the results in the context automatically writes
the result to output Dataset which is configured in the configure() method of the MapReduce program.

.. code:: java

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

Now that we have setup the data ingestion and processing components, the next step is to create a service to query
the processed data.

TopClientsService defines a simple HTTP REST endpoint to perform this query and return a response:

.. code:: java

  public class TopClientsService extends AbstractService {

    public static final byte [] DATASET_RESULTS_KEY = {'r'};

    @Override
    protected void configure() {
      setName("TopClientsService");
      addHandler(new ResultsHandler());
    }

    public static class ResultsHandler extends AbstractHttpServiceHandler {

      @UseDataSet(LogAnalyticsApp.DATASET_NAME)
      private ObjectStore<List<ClientCount>> topN;

      @GET
      @Path("/results")
      public void getResults(HttpServiceRequest request, HttpServiceResponder responder) {

        List<ClientCount> result = topN.read(DATASET_RESULTS_KEY);
        if (result == null) {
          responder.sendError(404, "Result not found");
        } else {
          responder.sendJson(200, result);
        }
      }
    }
  }


Build and Run
-------------

The LogAnalyticsApp can be built and packaged using Apache maven command:

  mvn clean package

Note that the remaining commands assume that the cdap-cli.sh script is available on your PATH. If this is not the case, please add it::

  export PATH=$PATH:<CDAP home>/bin

We can then deploy the application to a standalone CDAP installation::

  cdap-cli.sh deploy app target/cdap-mapreduce-guide-1.0.0.jar

Next, we will send some sample Apache access log event into the stream for processing::

  cdap-cli.sh send stream logEvents "255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /cdap.html HTTP/1.0\" 200 190 \" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n"
  cdap-cli.sh send stream logEvents "255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 200 102 \" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n"
  cdap-cli.sh send stream logEvents "255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /coopr.html HTTP/1.0\" 200 121 \" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n"
  cdap-cli.sh send stream logEvents "255.255.255.182 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 200 111 \" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n"
  cdap-cli.sh send stream logEvents "255.255.255.182 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 200 145 \" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n"


We can now start the MapReduce job to process the events that were ingested

  cdap-cli.sh start mapreduce LogAnalyticsApp.TopClientsMapReduce

The MapReduce job will take a couple of minutes to process.

We can now start the TopClients service and query the processing results::

  cdap-cli.sh start service LogAnalyticsApp.TopClientsService

  curl http://localhost:10000/v2/apps/LogAnalytics/services/TopClientsService/methods/results && echo

Example output::

[{"clientIP":"255.255.255.185","count":3},{"clientIP":"255.255.255.182","count":2}]

You have now learnt how to write MapReduce job to process events from a stream, write results to a DataSet and query
the results using services.

Extend This Example
-------------------
Now that you have the basics of MapReduce programs down, you can extend this example by:

* Writing a `workflow <http://docs.cask.co/cdap/current/en/dev-guide.html#workflow>`_ to schedule this MapReduce job every hour and process the last hour's data
* Store the results in a Timeseries data to analyze trends

Share and Discuss
---------------

Have a question? Discuss at `CDAP User Mailing List <https://groups.google.com/forum/#!forum/cdap-user>`_

.. |(AppDesign)| image:: docs/img/app-design.png
