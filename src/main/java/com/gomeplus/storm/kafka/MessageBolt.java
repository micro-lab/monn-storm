package com.gomeplus.storm.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gomeplus.storm.bean.Login;
import com.gomeplus.storm.bean.Message;

import net.sf.json.JSONObject;

/**
 * 
 * @author tanyongkai
 * @purpose 解析 messge,login,file,group 日志 
 * 
 *
 */
public class MessageBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(MessageBolt.class);
	private OutputCollector collector;
	private String[] lines;

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	public void execute(Tuple tuple) {
		String str0 = tuple.getString(0);
		// 通过σσmessage / σσlogin 判断消息类型,通过| 分割元组, 按原来的顺序封装一个新元组,发射
		if (str0.contains("σσmessage|")) {
			lines = str0.split("σσmessage\\|")[1].split("\\|");
			if (lines.length == 11 && StringUtils.isNumeric(lines[10])) { 
				collector.emit("message", new Values(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5],
						lines[6], lines[7], lines[8], lines[9], lines[10]));
				// kk
				// "messageid","fromId","platformtype","appid","grouptype","messageType","toId","groupId","manufactureType","directionType","messageTime"
				// hive
				// "messageid","fromid","grouptype","messagetype","toid","groupid","messagetime","platformtype","appid"
				try {
					Message message = new Message();
					message.setMessageId(lines[0]);
					message.setFromId(Long.valueOf(lines[1]));
					message.setPlatformType(Integer.valueOf(lines[2]));
					message.setAppId(lines[3]);
					message.setGroupType(Integer.valueOf(lines[4]));
					message.setMessgeType(Integer.valueOf(lines[5]));
					message.setToId(Long.valueOf(lines[6]));
					message.setGroupId(lines[7]);
					message.setManufactureType(Integer.valueOf(lines[8]));
					message.setDirectionType(Integer.valueOf(lines[9]));
					message.setMessageTime(Long.valueOf(lines[10]));
					message.setMessageDay(sdf.format(new Date(Long.valueOf(lines[10]))));
					JSONObject jsonObj = JSONObject.fromObject(message);
					jsonObj.put("action", "message");
					// collector.emit("log", new Values(jsonObj.toString()));
					collector.emit("log", new Values(new Object[] { jsonObj.toString() }));
				} catch (NumberFormatException e) {
					LOG.error("==message消息发送至kafka失败:"+e.getMessage());
				}

			}
			//历史遗留,9字段消息为不规范消息
/*			else if(lines.length == 9){
				String directionType="2";
				if(lines[6].equals("0")){
					directionType="1";
				}
				
				collector.emit("message", new Values(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5],
						lines[6], lines[7], "0",directionType, lines[8]));
				Message message = new Message();
				message.setMessageId(lines[0]);
				message.setFromId(Long.valueOf(lines[1]));
				message.setPlatformType(Integer.valueOf(lines[2]));
				message.setAppId(lines[3]);
				message.setGroupType(Integer.valueOf(lines[4]));
				message.setMessgeType(Integer.valueOf(lines[5]));
				message.setToId(Long.valueOf(lines[6]));
				message.setGroupId(lines[7]);
				message.setMessageTime(Long.valueOf(lines[8]));
				message.setMessageDay(sdf.format(new Date(Long.valueOf(lines[8]))));
				JSONObject jsonObj = JSONObject.fromObject(message);
				jsonObj.put("action", "message");
				// collector.emit("log", new Values(jsonObj.toString()));
				collector.emit("log", new Values(new Object[] { jsonObj.toString() }));
			}*/
		}
		
		//新版login
		else if (str0.contains("σσlogin|")) {
			lines = str0.split("σσlogin\\|")[1].split("\\|"); 
			if (lines.length == 5 && StringUtils.isNumeric(lines[4]) ) {  
				//kk "userId","platformtype","appid","version","loginTime"
				//hive "userid" ,"logtime" ,"platformtype" ,"appid"
				collector.emit("login", new Values(lines[0], lines[1], lines[2], lines[3],lines[4]));
//				collector.emit("login", new Values(lines[0], lines[3], lines[1], lines[2]));
				try {
					Login login = new Login();
					login.setAppId(lines[2]);
					login.setLogTime(Long.valueOf(lines[4]));
					login.setPlatformType(Integer.valueOf(lines[1]));
					login.setUserId(Long.valueOf(lines[0]));
					login.setLogDay(sdf.format(new Date(Long.valueOf(lines[4]))));
					JSONObject jsonObj = JSONObject.fromObject(login);
					jsonObj.put("action", "login");
					collector.emit("log", new Values(jsonObj.toString()));
				} catch (NumberFormatException e) {
					LOG.error("==login消息发送至kafka失败:"+e.getMessage());
				}
			}

		}
		//老版login
		/*
		 else if (str0.contains("σσlogin|")) {//"σσ" //"σσ"
			lines = str0.split("σσlogin\\|")[1].split("\\|");
			if (lines.length >= 4 && StringUtils.isNumeric(lines[3]) && !lines[3].equals("")
					&& Long.valueOf(lines[3]).longValue() >= 1493779421000L) {
				collector.emit("login", new Values(new Object[] { lines[0], lines[3], lines[1], lines[2] }));
				System.out.println("++++++++++++++++++++++++EMIT an login tuple");
				Login login = new Login();
				login.setAppId(lines[2]);
				login.setLogTime(Long.valueOf(lines[3]));
				login.setPlatformType(Integer.valueOf(lines[1]));
				login.setUserId(Long.valueOf(lines[0]));
				login.setLogDay(sdf.format(new Date(Long.valueOf(lines[3]).longValue())));
				JSONObject jsonObj = JSONObject.fromObject(login);
				jsonObj.put("action", "login");
				collector.emit("log", new Values(new Object[] { jsonObj.toString() }));
			}
		}*/
		//group
		else if (str0.contains("σσgroup|")) {
			lines = str0.split("σσgroup\\|")[1].split("\\|");
			collector.emit("group", new Values(lines[0], lines[1], lines[2], lines[3], lines[4]));
		}
		//file
		else if (str0.contains("σσfile|")) {
			lines = str0.split("σσfile\\|")[1].split("\\|");
			collector.emit("file", new Values(lines[0], lines[1], lines[2], lines[3], lines[4], lines[5], lines[6],
					lines[7], lines[8]));
		}
		collector.ack(tuple);

	}
	
	/**
	 * 定义tuple 字段
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("message",new Fields("messageid","fromId","platformtype","appid","grouptype","messageType","toId","groupId","manufactureType","directionType","messageTime"));
//		declarer.declareStream("message", new Fields("messageid", "fromid", "grouptype", "messagetype", "toid","groupid", "messagetime", "platformtype", "appid")); //old
		declarer.declareStream("login", new Fields("userId","platformtype","appid","version","loginTime"));
//		declarer.declareStream("login", new Fields("userid" ,"logtime" ,"platformtype" ,"appid"));  //old
		declarer.declareStream("group",new Fields("appid","grouptype","groupId","operationType","operationTime"));
		declarer.declareStream("file",new Fields("appId","groupType","groupId","fileId","fileType","fileSize","updownload","operationType","operationTime"));
		declarer.declareStream("log", new Fields("message"));

	}
	
}
