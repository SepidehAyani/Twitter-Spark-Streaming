package com.company;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;


public class TwitterPopularLinks implements Serializable{

  private static final long serialVersionUID = 1L;

  /** Creates Spark Configuration Instance**/
  private static SparkConf conf = new SparkConf().setAppName("Twitter Streaming").setMaster("local[2]");
  private static final String consumerKey ="Your Consumer Key";
  private static final String consumerSecret = "Your Consumer Secret";
  private static final String accessTokenKey = "Your Access Token Key";
  private static final String accessTokenKey_secret = "Your Access Token Key Secret";

  //We need to set an HDFS for periodic checkpointing of the intermediate data.
  //private String checkPoint = "/home/Documents/...";

  public static void main(String args[]){

    TwitterPopularLinks stream = new TwitterPopularLinks();
    stream.sparkStreaming(conf);
  }

  public void sparkStreaming(SparkConf conf ){

    ///Creates Streaming Context
    JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

    //Create a Twitter
    Twitter twitter = new TwitterFactory().getInstance();
    twitter.setOAuthConsumer(consumerKey, consumerSecret);
    twitter.setOAuthAccessToken(new AccessToken(accessTokenKey,accessTokenKey_secret));

    JavaDStream<Status> stream = TwitterUtils.createStream(jsc,twitter.getAuthorization());

    JavaDStream<String> words = stream.map(
            new Function<Status, String>() {
              public String call(Status status) { return status.getText(); }
            }
    );

    JavaDStream<String> statuses = words.flatMap(
            new FlatMapFunction<String, String>() {
              public Iterable<String> call(String in) {
                return Arrays.asList(in.split(" "));
              }
            }
    );
    //Get the stream of hashtags from the stream of tweets
    JavaDStream<String> hashTags = statuses.filter(
            new Function<String, Boolean>() {
              public Boolean call(String word) { return word.startsWith("#"); }f
            }
    );
    //Count the hashtags over a 5 minute window
    JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
            new PairFunction<String, String, Integer>() {
              public Tuple2<String, Integer> call(String in) {
                return new Tuple2<String, Integer>(in, 1);
              }
            }
    );
    //count these hashtags over a 5 minute moving window
    JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
            new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer i1, Integer i2) { return i1 + i2; }
            },
            new Function2<Integer, Integer, Integer>() {
              public Integer call(Integer i1, Integer i2) { return i1 - i2; }
            },
            new Duration(60 * 5 * 1000),
            new Duration(1 * 1000)
    );

    counts.print();
    //jsc.checkpoint(checkPoint);
    jsc.start();
    jsc.awaitTermination();

  }
}