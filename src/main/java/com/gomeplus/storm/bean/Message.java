package com.gomeplus.storm.bean;

import java.io.Serializable;

public class Message implements Serializable {
	private String messageId;
	private Long fromId;

	private Integer platformType;

	private String appId;

	private Integer groupType;
	private Integer messgeType;
	private Long toId;
	private String groupId;
	private Long messageTime;
	private Integer manufactureType;

	private Integer directionType;
	private String messageDay;

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public Long getFromId() {
		return fromId;
	}

	public void setFromId(Long fromId) {
		this.fromId = fromId;
	}

	public Integer getPlatformType() {
		return platformType;
	}

	public void setPlatformType(Integer platformType) {
		this.platformType = platformType;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public Integer getGroupType() {
		return groupType;
	}

	public void setGroupType(Integer groupType) {
		this.groupType = groupType;
	}

	public Integer getMessgeType() {
		return messgeType;
	}

	public void setMessgeType(Integer messgeType) {
		this.messgeType = messgeType;
	}

	public Long getToId() {
		return toId;
	}

	public void setToId(Long toId) {
		this.toId = toId;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public Long getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(Long messageTime) {
		this.messageTime = messageTime;
	}

	public Integer getManufactureType() {
		return manufactureType;
	}

	public void setManufactureType(Integer manufactureType) {
		this.manufactureType = manufactureType;
	}

	public Integer getDirectionType() {
		return directionType;
	}

	public void setDirectionType(Integer directionType) {
		this.directionType = directionType;
	}

	public String getMessageDay() {
		return messageDay;
	}

	public void setMessageDay(String messageDay) {
		this.messageDay = messageDay;
	}

	@Override
	public String toString() {
		return messageId + "|" + fromId + "|" + platformType + "|" + appId + "|" + groupType + "|" + messgeType + "|"
				+ toId + "|" + groupId + "|" + messageTime;
	}
}