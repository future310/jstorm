package com.com.xuechao.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt(),2)
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt(), 2)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", "/home/xuechao/stormtest.txt");
        conf.setDebug(true);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topology", conf, builder.createTopology());
        Thread.sleep(3000);
        cluster.shutdown();
    }
}
