package com.quark.datastream.runtime.topologygenerator.controller.base;

import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quark.datastream.runtime.common.utils.ResponseBody;

public class BaseController {

	private static ObjectMapper mapper = null;

	public synchronized static ObjectMapper getMapper() {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		return mapper;
	}

	protected static ResponseBody getResponse(Integer code, Object responseBody, String responseMsg) {
		return new ResponseBody(code, responseBody, responseMsg);
	}

	protected static ResponseBody getSuccessResponse() {
		return new ResponseBody(HttpStatus.OK.value(), null, null);
	}

	protected static ResponseBody getSuccessResponse(Object responseBody) {
		return new ResponseBody(HttpStatus.OK.value(), responseBody, null);
	}

	protected static ResponseBody getErrorResponse() {
		return new ResponseBody(HttpStatus.INTERNAL_SERVER_ERROR.value(), null, null);
	}

	protected static ResponseBody getErrorResponse(String responseMsg) {
		return new ResponseBody(HttpStatus.INTERNAL_SERVER_ERROR.value(), null, responseMsg);
	}
}
