package storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue;
    TwitterStream _twitterStream;
    String _username;
	String _pwd;
	String _query;
	
	public TwitterSpout(String twitterUser, String twitterPwd,
			String query){
		_username = twitterUser;
		_pwd = twitterPwd;
		_query = query;
	}
	
	public void startStream(){
		StatusListener listener = new StatusListener() {
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onStatus(Status status) {
				// TODO Auto-generated method stub
				queue.offer(status);
			}

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}
        };
        _twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setUser(_username).setPassword(_pwd).build()).getInstance();
	    FilterQuery filter = new FilterQuery();
	    String keywords[] = {_query};
	    filter.track(keywords);
	    _twitterStream.addListener(listener);
	    _twitterStream.filter(filter);
	}
	
	public void nextTuple() {
		Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(10);
        } else {
            _collector.emit(new Values(ret.getText()));
        }
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}    
	
	public void close() {
        _twitterStream.shutdown();
    }

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<Status>(1000);
		startStream();
		
	}
}