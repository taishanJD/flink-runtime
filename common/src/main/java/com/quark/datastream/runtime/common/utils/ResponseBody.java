package com.quark.datastream.runtime.common.utils;

import java.io.Serializable;

public class ResponseBody implements Serializable {

	private static final long serialVersionUID = 8641946526457559598L;

	private Integer code;

	private Object responseBody;

	private String responseMsg;

	public ResponseBody(Integer code, Object responseBody, String responseMsg) {
		this.code = code;
		this.responseBody = responseBody;
		this.responseMsg = responseMsg;
	}

	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}

	public Object getResponseBody() {
		return responseBody;
	}

	public void setResponseBody(Object responseBody) {
		this.responseBody = responseBody;
	}

	public String getResponseMsg() {
		return responseMsg;
	}

	public void setResponseMsg(String responseMsg) {
		this.responseMsg = responseMsg;
	}

}
