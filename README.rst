Batch Data Processing With CDAP
===============================

`MapReduce <http://research.google.com/archive/mapreduce.html>`_ is the most popular paradigm for processing large
amounts of data in a reliable and fault-tolerant manner.In this guide you will learn how to batch process data using
MapReduce in the `Cask Data Application Platform (CDAP). <http://cdap.io>`_

What You Will Build
-------------------

This guide will take you through building a CDAP application that ingests data from Apache access log, computes
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

Following sections will guide you through building an application from scratch. If you are interested in deploying and
running the application right away, you can download its source code and binaries from `here <placeholder..>`_ In that
case feel free to skip the next two sections and jump right to Build and Run Application section.

Application Design
------------------

The application processes Apache access logs data ingested into the Stream using MapReduce job. The data can be ingested
into a Stream continuously in real-time or in batches, which doesn’t affect the ability to consume it by MapReduce.

MapReduce job extract necessary information from raw web logs and computes top 10 Client IPs. The results of the
computation are stored in a Dataset.

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
  ./src/main/java/co/cask/cdap/guides/TopClientsService.java
  ./src/main/assembly/assembly.xml
  ./src/test/java/co/cdap/guides/LogAnalyticsAppTest.java


The CDAP application is identified by LogAnalyticsApp class. This class extends an
`AbstractApplication <http://docs.cdap.io/cdap/2.5.0/en/javadocs/co/cask/cdap/api/app/AbstractApplication.html>`_,
and overrides the configure() method in order to define all of the application components:

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
    addService(new TopClientsService());
    try {
      DatasetProperties props = ObjectStores.objectStoreProperties(Types.listOf(ClientCount.class),
                                                                   DatasetProperties.EMPTY);
      createDataset(DATASET_NAME, ObjectStore.class, props);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }
}

.. |(AppDesign)| image:: docs/img/app-design.png