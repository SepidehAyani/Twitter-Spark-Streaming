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

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;


public class TwitterPopularLinks {

  private static final Logger log = Logger.getLogger(TwitterPopularLinks.class);

  public static void main(String[] args) throws ConfigurationException {
    TwitterPopularLinks workflow = new TwitterPopularLinks();
    log.setLevel(Level.DEBUG);

    CompositeConfiguration conf = new CompositeConfiguration();
    conf.addConfiguration(new PropertiesConfiguration("spark.properties"));

    try {
      workflow.run(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void run(CompositeConfiguration conf) {
    // Spark conf
    SparkConf sparkConf = new SparkConf().setAppName("Twitter Spark").setMaster(conf.getString("spark.master"))
            .set("spark.serializer", conf.getString("spark.serializer"));
    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

    // Twitter4J and configuring twitter4J.properties
    Configuration twitterConf = ConfigurationContext.getInstance();
    Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

    String consumerKey = "<MU21bDncz8Ixab7UKFoSZQDeO>";
    String consumerSecret = "<k2cwpee6JJeqVPMeo3fdzsS71x81g4j8ELZLvT5pCizS6G9POS\n>";
    String accessToken = "<491057026-rXLZ1sncKe9jn2hvg6u35t8V6nFPGRCsTEwc6pEO>";
    String accessTokenSecret = "<HrzYdl2HMmASpy3Sz304Cf8DUdlzmrzVr3IDq9MMMaMBi>";

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
    System.setProperty("twitter4j.oauth.accessToken", accessToken);
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

    // Create twitter stream
    String[] filters = { "https://", "http://" };
    TwitterUtils.createStream(jsc, twitterAuth, filters).print();
    // Start the computation
    jsc.start();
    jsc.awaitTermination();
  }
}