# flume-interceptor-analytics

## What is Apache Flume?

Flume is a distributed service for efficiently collecting, aggregating, and moving large amounts of data to a centralised data store. It's architecture is based on streaming data flows and it uses a simple extensible data model that allows for online analytic application. It is robust and fault tolerant with tuneable reliability mechanisms and many failover and recovery mechanisms.

A unit of data in Flume is called an event, and events flow through one or more Flume agents to reach their destination. An event has a byte payload and an optional set of string attributes. An agent is a Java process that hosts the components through which events flow. The components are a combination of sources, channels, and sinks.

A Flume source consumes events delivered to it by an external source. When a source receives an event, it stores it into one or more Flume channels. A channel is a passive store that keeps the event until it's consumed by a Flume sink. The sink removes the event from the channel and puts it into an external repository (i.e. HDFS or HBase) or forwards it to the source of the next agent in the flow. The source and sink within a given agent run asynchronously, with the events staged in the channel.

Flume agents can be chained together to form multi-hop flows. This allows flows to fan-out and fan-in, and for contextual routing and backup routes to be configured.

For more information, see the [Apache Flume User Guide](http://flume.apache.org/FlumeUserGuide.html).

## What are Flume Interceptors?

Interceptors are part of Flume's extensibility model. They allow events to be inspected as they pass between a source and a channel, and the developer is free to modify or drop events as required. Interceptors can be chained together to form a processing pipeline.

Interceptors are classes that implement the `org.apache.flume.interceptor.Interceptor` interface and they are defined as part of a source's configuration, for example:

    a1.sources = s1
    a1.sources.s1.interceptors = i1 i2
    a1.sources.s1.interceptors.i1.type = org.apache.flume.interceptor.FirstInterceptor$Builder
    a1.sources.s1.interceptors.i2.type = org.apache.flume.interceptor.SecondInterceptor$Builder

For more information, see [Flume Interceptors](http://flume.apache.org/FlumeUserGuide.html#flume-interceptors).

## What is flume-interceptor-analytics?

The aim of this project is to build a library of interceptors that exploit Flume's extensibility model to apply real-time analytics to data flows. Analysing data in-flight reduces response times and allows consumers to view information as it happens.

## The streaming topN example

The streaming topN example demonstrates how to use a chain of interceptors to compute a near real-time list of the 10 most popular hashtags from a continuous stream of twitter status updates.

Cloudera's `TwitterSource` is used to connect to the twitter firehose and emit events that match a set of search keywords. A series of Flume interceptors is then used to extract, count, and compute a rolling topN of the hashtags used in the status updates.

First, `HashtagRollingCountInterceptor` extracts and counts the hashtags in a sliding window style, and then `HashtagTopNInterceptor` takes the counters and computes the topN. `PeriodicEmissionSource` is a separate source that connect to `HashtagTopNInterceptor` and periodically emits the topN list.

Much more information about the streaming topN example and the interceptors can be found on the wiki, including how to scale out the analytic to handle high-volume, high-velocity data flows:

* [The streaming topN example](https://github.com/jrkinley/flume-interceptor-analytics/wiki/The-streaming-topN-example)
* [Scaling out streaming topN](https://github.com/jrkinley/flume-interceptor-analytics/wiki/Scaling-out-streaming-topN)

## Getting started

1. **[Install Flume](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Installation-Guide/cdh4ig_topic_12.html)**

2. **Build flume-interceptor-analytics**

    <pre>
    $ git clone https://github.com/jrkinley/flume-interceptor-analytics.git
    $ cd flume-interceptor-analytics
    $ mvn clean package
    $ ls target
    interceptor-analytics-0.0.1-SNAPSHOT.jar
    </pre>

3. **Build or download Cloudera's custom Flume source**

    <pre>
    $ git clone https://github.com/cloudera/cdh-twitter-example.git
    $ cd cdh-twitter-example/flume-sources
    $ mvn clean package
    $ ls target
    flume-sources-1.0-SNAPSHOT.jar
    </pre>

    or

    <pre>$ curl -O http://files.cloudera.com/samples/flume-sources-1.0-SNAPSHOT.jar</pre>

4. **Add JARs to the Flume classpath**

    <pre>
    $ sudo cp /etc/flume-ng/conf/flume-env.sh.template /etc/flume-ng/conf/flume-env.sh
    $ vi /etc/flume-ng/conf/flume-env.sh
    FLUME_CLASSPATH=/path/to/file/interceptor-analytics-0.0.1-SNAPSHOT.jar:/path/to/file/flume-sources-1.0-SNAPSHOT.jar
    </pre>

    Edit the `flume-env.sh` file and uncomment the `FLUME_CLASSPATH` line.
    Enter the paths to `interceptor-analytics-0.0.1-SNAPSHOT.jar` and `flume-sources-1.0-SNAPSHOT.jar` separating them with a colon.

5. **Set the Flume agent name to AnalyticsAgent**

    <pre>
    $ vi /etc/default/flume-ng-agent
    FLUME_AGENT_NAME=AnalyticsAgent
    </pre>

6. **Set the Flume agent configuration**

    Copy the example agent configuration from `/src/main/resources/flume-topn-example.conf` to `/etc/flume-ng/conf/flume.conf`.
    
    Add your authentication details for accessing the twitter streaming API:

    <pre>
    AnalyticsAgent.sources.Twitter.consumerKey = [required]
    AnalyticsAgent.sources.Twitter.consumerSecret = [required]
    AnalyticsAgent.sources.Twitter.accessToken = [required]
    AnalyticsAgent.sources.Twitter.accessTokenSecret = [required]
    </pre>

    Set where you would like to store the status updates in HDFS:

    <pre>
    AnalyticsAgent.sinks.TwitterHDFS.hdfs.path = hdfs://[required]:8020/user/flume/tweets/%Y/%m/%d/%H
    </pre>

    Set where you would like to store the topN results in HDFS:

    <pre>
    AnalyticsAgent.sinks.TopNHDFS.hdfs.path = hdfs://[required]:8020/user/flume/topn/%Y/%m/%d/%H
    </pre>

7. **Create HDFS directories**

    <pre>
    $ hadoop fs -mkdir /user/flume/tweets
    $ hadoop fs -mkdir /user/flume/topn
    </pre>

8. **Start the Flume agent**

    <pre>
    $ sudo /etc/init.d/flume-ng-agent start
    $ tail -100f /var/log/flume-ng/flume.log
    </pre>