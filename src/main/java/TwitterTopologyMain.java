import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.TwitterWordNormalizer;
import bolts.WordCounter;
import bolts.WordNormalizer;
import spouts.SampleApiStreamingSpout;
import spouts.WordReader;

import java.util.HashMap;
import java.util.Map;


public class TwitterTopologyMain {
	public static void main(String[] args) throws InterruptedException {

        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-reader",new SampleApiStreamingSpout());
		builder.setBolt("word-normalizer", new TwitterWordNormalizer())
			.shuffleGrouping("twitter-reader");
		builder.setBolt("word-counter", new WordCounter(),1)
			.fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
		Config conf = new Config();
        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.setDebug(false);

        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
//		Thread.sleep(10000);
//		cluster.shutdown();
	}
}
