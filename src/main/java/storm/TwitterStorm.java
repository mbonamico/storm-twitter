package storm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.MongoBolt;
import storm.spout.TwitterSpout;

public class TwitterStorm {
	
	public static class SplitSentence extends ShellBolt implements IRichBolt {
        
        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }  
	
	public Properties getProperties() throws IOException{
		Properties props = new Properties();
		props.load(getClass().getResourceAsStream("/twitter_properties" ));
		return props;
	}
	public static void main(String[] args) throws Exception {
	    TwitterStorm twitterStorm =  new TwitterStorm();  
		Properties properties = twitterStorm.getProperties();
		TopologyBuilder builder = new TopologyBuilder();
		
        MongoBolt mongoBolt = new MongoBolt("localhost", 27017, "storm", "tweets");
        
        builder.setSpout("spout", new TwitterSpout(properties.getProperty("twitter_user"),
        		properties.getProperty("twitter_pwd"),
        		properties.getProperty("twitter_query")),
        		1);
        
        builder.setBolt("split", new SplitSentence(), 4)
                .shuffleGrouping("spout");
        
        builder.setBolt("count", mongoBolt, 12)
        .fieldsGrouping("split", new Fields("word"));
        
        Config conf = new Config();
        conf.setDebug(true);

        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {        
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
        }
    }
}
