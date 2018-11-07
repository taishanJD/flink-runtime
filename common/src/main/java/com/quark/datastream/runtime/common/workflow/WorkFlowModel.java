package com.quark.datastream.runtime.common.workflow;

import java.io.Serializable;

public class WorkFlowModel implements Serializable {

	private static final long serialVersionUID = -820691217311627656L;
	private String host;

	private String engineType;

	private String engineId;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getEngineType() {
		return engineType;
	}

	public void setEngineType(String engineType) {
		this.engineType = engineType;
	}

	public String getEngineId() {
		return engineId;
	}

	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

}
