package topologies;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import bolts.Bolt;
import spouts.Spout;

public class ReadAndWriteFile {
    public static void main(String[] args) throws Exception{
        //Build topologies
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("YF-Spout", new Spout(), 3);
        builder.setBolt("YF-Bolt", new Bolt(), 3).shuffleGrouping("YF-Spout");

        StormTopology topology = builder.createTopology();

        //Configure
        Config conf = new Config();
        conf.put("fileToWrite", "/home/joao.machado/Área de Trabalho/output.txt");

        //Submit topologies
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("STOCK-PRICE-TRACKER", conf, topology);
            Thread.sleep(10000);
        } finally {
            cluster.shutdown();
        }
    }

}
