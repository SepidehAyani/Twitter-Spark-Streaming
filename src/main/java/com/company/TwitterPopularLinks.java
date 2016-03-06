/**
 * Created by sepidehayani on 3/4/16.
 */
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

import java.util.Arrays;
import org.apache.spark.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;


public class TwitterPopularLinks {
  public static void main(String[] args) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularLinks <consumer key> <consumer secret> " +
              "<access token> <access token secret> [<filters>]");
      System.exit(1);
    }

    String consumerKey = args[0];
    String consumerSecret = args[1];
    String accessToken = args[2];
    String accessTokenSecret = args[3];
    String[] filters = new String[] {"#"};

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

    //Configure the Streaming Context
    SparkConf sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[*]");
    //Create the context with 2 seconds batch size
    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new org.apache.spark.streaming.Duration(10000));

    // start receiving a stream of tweets ...
    JavaReceiverInputDStream<String> tweets = TwitterUtils.createDirectStream(jsc, filters);
    JavaDStream<String> links = tweets.flatMap(
            new FlatMapFunction<String,String>() {
              public Iterable<String> call(String tweet){
                return Arrays.asList(tweet.split(" "));
              }
            }
    );

    JavaPairDStream<String, Integer> TwitterPopularLinks = links.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String one) {
         return new Tuple2<String, Integer>(one, 1);
      }

            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer T1, Integer T2) {
        return T1 + T2;
      }
    });
    JavaDStream<String> st1 = links.filter(new Function<String, Boolean>(){
      @Override
      public Boolean call(String st){return st.startsWith("#");
              }
            }
    );
    System.out.println("links");
    links.print();
    st1.count().print();
    jsc.start();
  }
}