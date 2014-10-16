Batch Data Processing With CDAP
===============================

`MapReduce <http://research.google.com/archive/mapreduce.html>`_ is the most popular paradigm for processing large
amounts of data in a reliable and fault-tolerant manner. In this guide you will learn how to batch process data using
MapReduce in the `Cask Data Application Platform (CDAP). <http://cdap.io>`_

What You Will Build
-------------------

This guide will take you through building a CDAP application that ingests Apache access log events, computes
top 10 client IPs that sends requests in the last hour and query the results. You will:

* Build MapReduce program to process events from Apache access log
* Use `Dataset <http://docs.cask.co/cdap/current/en/dev-guide.html#datasets>`_ to persist the result of a
  MapReduce program
* Build a `Service <http://docs.cask.co/cdap/current/en/dev-guide.html#services>`_ to serve the results via HTTP


What You Will Need
------------------

* `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_
* `Apache Maven 3.0+ <http://maven.apache.org/>`_
* `CDAP SDK <http://docs.cdap.io/cdap/current/en/getstarted.html#download-and-setup>`_

Let’s Build It!
---------------

Following sections will guide you through building an application from scratch. You can download its source code and binaries from `here <placeholder..>`_ 

Application Design
------------------

The application will assume that we are processing Apache access log events from Apache servers. The log events can be ingested
into a Stream continuously in real-time or in batches, which doesn’t affect the ability to consume it by MapReduce.

MapReduce job extract necessary information from raw logs and computes top 10 Client IPs by traffic in a specific time range.
The results of the computation are persisted in a Dataset.

The application also contains a Service that exposes HTTP endpoint to access data stored in a Dataset.

|(AppDesign)|


Implementation
--------------

The first step is to get our application structure set up.  We will use a standard Maven project structure for all of
the source code files::

  ./pom.xml
  ./README.rst
  ./src/main/java/co/cask/cdap/guides/ClientCount.java
  ./src/main/java/co/cask/cdap/guides/LogAnalyticsApp.java
  ./src/main/java/co/cask/cdap/guides/package-info.java
  ./src/main/java/co/cask/cdap/guides/TopClientsMapReduce.java
  ./src/main/java/co/cask/cdap/guides/IPMapper.java
  ./src/main/java/co/cask/cdap/guides/TopNClientsReducer.java
  ./src/main/java/co/cask/cdap/guides/CountsCombiner.java
  ./src/main/java/co/cask/cdap/guides/TopClientsService.java
  ./src/main/assembly/assembly.xml
  ./src/test/java/co/cdap/guides/LogAnalyticsAppTest.java


The CDAP application is identified by LogAnalyticsApp class. This class extends an
`AbstractApplication <http://docs.cdap.io/cdap/2.5.1/en/javadocs/co/cask/cdap/api/app/AbstractApplication.html>`_,
and overrides the ``configure()`` method in order to define all of the application components:

.. code:: java

  public class LogAnalyticsApp extends AbstractApplication {
    
    public static final String DATASET_NAME = "resultStore";
    public static final byte [] DATASET_RESULTS_KEY = {'r'};
    
    @Override
    public void configure() {
      setName("LogAnalyticsApp");
      setDescription("An application that computes top 10 clientIPs from Apache access log data");
      addStream(new Stream("logEvent"));
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

The LogAnalyticsapplication defines a new `Stream <http://docs.cdap.io/cdap/current/en/dev-guide.html#streams>`_ to ingest the Apache access log events.
The log events can be ingested into CDAP using a RESTful API. Once the data is ingested into the stream the events
can be processed in real-time or batch. In our application, we will process the events in batch using the
TopClientsMapReduce program and compute top10 client IPs in a specific time-range.

The results of the MapReduce job is persisted into a Dataset, the application uses createDataset method to define
the Dataset. Finally, the application adds a service to query the results from the Dataset.

Let's take a closer look at the MapReduce program.

The TopClientsMapReduce job extends an 
`AbstractMapReduce <http://docs.cdap.io/cdap/2.5.1/en/javadocs/co/cask/cdap/api/mapreduce/AbstractMapReduce.html>`_
class and overrides the ``configure()`` and ``beforeSubmit()`` methods.

* ``configure()`` method configures a MapReduce job, by returning an instance of
`MapReduceSpecification <http://docs.cdap.io/cdap/2.5.1/en/javadocs/co/cask/cdap/api/mapreduce/MapReduceSpecification.html>`_. The MapReduce
job name, description and output Dataset are configured in the example.

* ``beforeSubmit()`` method is invoked at runtime, before the MapReduce job is executed. Here, you will have access to the
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
      StreamBatchReadable.useStreamInput(context, "logEvent", startTime, endTime);
    }
  }


The next step is to implement the Mapper and Reduce classes. The Mapper and Reducer classes extend from the standard
`Hadoop APIs<http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/package-summary.html>`_

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

Now that we have setup the data ingestion and processing components, the next step is to create a service to query
the processed data.

TopClientsService defines a simple HTTP REST endpoint to perform this query and return a response:

.. code:: java

  public class TopClientsService extends AbstractService {

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

        List<ClientCount> result = topN.read(LogAnalyticsApp.DATASET_RESULTS_KEY);
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

The LogAnalyticsApp can be built and packaged using standard Apache maven command:

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

The MapReduce job will take a couple of minutes to process the sample data ingested.

We can now start the TopClients service and check the service calls::

  cdap-cli.sh start service LogAnalyticsApp.TopClientsService

  curl http://localhost:10000/v2/apps/LogAnalytics/services/TopClientsService/methods/results && echo

Example output::

  placeholder

You have now learnt how to write MapReduce job to process events from a stream, write results to a DataSet and query
the results using services.

Extend This Example
-------------------
Now that you have the basics of MapReduce programs down, you can extend this example by:

* Writing a workflow to schedule this MapReduce job every hour and process the last hour's data
* Store the results in a Timeseries data for each run of the MapReduce job

Share and Discuss
---------------

Have a question? Discuss at `CDAP User Mailing List <https://groups.google.com/forum/#!forum/cdap-user>`_

.. |(AppDesign)| image:: docs/img/app-design.png
