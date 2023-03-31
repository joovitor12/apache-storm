package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) throws Exception {
        String inputString = "hello world\n" +
                "goodbye world\n" +
                "hello storm\n";

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter())
                .fieldsGrouping("word-normalizer", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountTopology", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
