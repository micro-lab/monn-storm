package com.gomeplus.storm.kafka;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gomeplus.storm.bean.ErrorLog;

import net.sf.json.JSONObject;

/**
 * 
 * @author tanyongkai
 * @purpose 解析服务日志
 * 
 *
 */
public class ErrorBolt extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(ErrorBolt.class);
	private OutputCollector collector;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	public static enum SERVER_TYPE {
		LOGIN("loginserver", 1), 	// 登陆服务
		LOGIC("logicserver", 2), 	// 逻辑服务
		PLATFORM("im-platform", 3), //
		API("center-im-api", 4);	//
		private String name;
		private int id;

		private SERVER_TYPE(String name, int index) {
			this.name = name;
			this.id = index;
		}

		public static int getId(String name) {
			for (SERVER_TYPE c : SERVER_TYPE.values()) {
				if (name.equals(c.getName())) {
					return c.id;
				}
			}
			return 0;
		}

		public static int getMinId() {
			int id = 1;
			for (SERVER_TYPE c : SERVER_TYPE.values()) {
				id = Math.min(id, c.getId());
			}
			return id;
		}

		public static int getMaxId() {
			int id = 1;
			for (SERVER_TYPE c : SERVER_TYPE.values()) {
				id = Math.max(id, c.getId());
			}
			return id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

	}

	@Override
	public void execute(Tuple tuple) {
		String str0 = tuple.getString(0);
		if (str0.startsWith("{")) {
			try {
				// 通过{}获取服务器的信息，包含IP，服务类型,端口号,无端口号时，使用字符串“0”代替，任何一个为空时，抛出异常，丢弃该元组
				String[] macheinfo = str0.substring(1, str0.indexOf("}")).split("_");
				String ip = macheinfo[1];
				String port = macheinfo[2].split("\\.")[0].equals("noport") ? "0" : macheinfo[2].split("\\.")[0];
				String serverType = macheinfo[0];
			    
				// 获取服务类型ID
				int serverTypeId = SERVER_TYPE.getId(serverType);

				String level = "";
				String errorStackTrace = "";
				String msg = "";
				long logtime = 0;
				String logDate;

				// 获取日志级别
				if (str0.contains("[ERROR]")) {
					level = "ERROR";
				} else if (str0.contains("[WARN]")) {
					level = "WARN";
				} else if (str0.contains("[INFO]")) {
					level = "INFO";
				} else if (str0.contains("[DEBUG]")) {
					level = "DEBUG";
				}

				// 获取23位的时间戳,在 }[ 之后取23 位，如果取值失败，则判定该条log为无效记录
				String time = str0.split("}")[1].substring(1, 24);
				// parse error time,throws an UnParseException if time is not a number
				// 获取一个长整形的时间戳,可能跑出时间转换异常，丢弃该条记录
				logtime = sdf.parse(time).getTime();
				// 获取一个字符串时间
				logDate = time.substring(0, 10);
				
//				if(serverType==null || serverType.equals("") || serverType.contains("api") || serverType.contains("platform")){
//			    	LOG.error("=====serverType: "+serverType+" ===== "+ time);
//			    }
				
				// " - "之后，换行之前的信息为msg,msg有可能为空
				if (str0.contains(" - ")) {
					String msgs = str0.split(" - ")[1];
					if (msgs.contains("\n")) {
						msg = msgs.split("\n")[0];
					} else {
						msg = msgs;
					}
				}

				// 换行符"\n"之后的信息为堆栈信息,可为空
				if (str0.contains("\n")) {
					errorStackTrace = str0.substring(str0.indexOf("\n") + 1);
				}

				// level不可为空,logtime 需为正常时间,serverType 需在枚举类型之内
				if (!level.equals("") && logtime != 0 && serverTypeId >= SERVER_TYPE.getMinId()
						&& serverTypeId <= SERVER_TYPE.getMaxId()) {
					ErrorLog errorLog = new ErrorLog();
					errorLog.setIp(ip);
					errorLog.setServerType(SERVER_TYPE.getId(serverType));
					errorLog.setPort(Integer.parseInt(port));
					errorLog.setLevel(level);
					errorLog.setLogTime(logtime);
					errorLog.setErrorStackTrace(errorStackTrace);
					errorLog.setMsg(msg);
					errorLog.setLogDate(logDate);

					JSONObject jsonObj = JSONObject.fromObject(errorLog);
					collector.emit(new Values(jsonObj.toString()));
					collector.ack(tuple);
				} else {
					LOG.error("Incomplete log:logtime {},level {},server_type {}", logtime, level,
							SERVER_TYPE.getId(serverType));
				}
			} catch (Exception e) {
				LOG.error(e.getMessage());
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("errorinfo"));

	}

}
