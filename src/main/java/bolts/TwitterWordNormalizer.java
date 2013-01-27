package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;

public class TwitterWordNormalizer extends BaseBasicBolt {

    static JSONParser jsonParser = new JSONParser();

	public void cleanup() {}

	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
        String json = (String)input.getValueByField("tweet");
        JSONObject jsonObject = null;

        try {
            jsonObject = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if(jsonObject.containsKey("text")){
            String textStr = (String) jsonObject.get("text");
//            String location = (String)jsonObject.get("lang");
//            System.out.println(textStr);

            String[] words = textStr.split(" ");
            for(String word : words){
                word = word.trim();
                if(!word.isEmpty()){
                    word = word.toLowerCase();

//                    List a = new ArrayList();
//                    a.add(tuple);

//                    if(this.LANG.equals(location) && !ignoreWords.contains(word)) {
                        collector.emit(new Values(word));
//                    }
                }
            }

//            collector.ack(tuple);
        }
	}
	

	/**
	 * The bolt will only emit the field "word" 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
