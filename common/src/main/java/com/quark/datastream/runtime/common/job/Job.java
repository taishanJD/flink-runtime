package com.quark.datastream.runtime.common.job;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.quark.datastream.runtime.common.Format;
import com.quark.datastream.runtime.common.workflow.WorkflowData;
import com.quark.datastream.runtime.common.workflow.WorkflowData.EngineType;

@JsonInclude(Include.NON_NULL)
public class Job extends Format {

	private static final long serialVersionUID = -6295845331481079564L;

	private String jobId;
	private String engineId;
	private String engineType;
	private State state;
	private String host;
	private int port;
	private Map<String, Object> config;
	private transient WorkflowData workflowData = null;

	public Job() {

	}

	public Job(String engineId) {
		this.engineId = engineId;
	}

	public static Job create(String engineId) {
		if (StringUtils.isEmpty(engineId)) {
			throw new RuntimeException("engineId id is null.");
		}
		return new Job(engineId);
	}

	public static Job create(WorkflowData workflowData) {
		if (workflowData == null) {
			throw new RuntimeException("Workflow data is null.");
		}
		Job job = new Job();
		job.setJobId(UUID.randomUUID().toString());
		job.setWorkflowData(workflowData);
		job.setConfig(workflowData.getConfig());
		try {
			job.setEngineType(workflowData.getEngineType().name());
		} catch (Exception e) {
			job.setEngineType(EngineType.UNKNOWN.name());
		}
		return job;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public String getEngineType() {
		return engineType;
	}

	public void setEngineType(String engineType) {
		this.engineType = engineType;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getEngineId() {
		return engineId;
	}

	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	@JsonIgnore
	public Map<String, Object> getConfig() {
		return config;
	}

	@SuppressWarnings("unchecked")
	@JsonIgnore
	public <T> T getConfig(String key) {
		return (T) config.get(key);
	}

	@JsonIgnore
	public void setConfig(Map<String, Object> config) {
		if (config == null) {
			throw new RuntimeException("Invalid config");
		}
		this.config = config;
	}

	@JsonIgnore
	public void addConfig(String key, Object value) {
		this.config.put(key, value);
	}

	@JsonProperty("config")
	public String getConfigStr() {
		try {
			return mapper.writeValueAsString(config);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@JsonProperty("config")
	public void setConfigStr(String configStr) {
		try {
			if (StringUtils.isEmpty(configStr)) {
				throw new RuntimeException("Invalid config");
			}
			this.config = mapper.readValue(configStr, new TypeReference<Map<String, Object>>() {
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@JsonIgnore
	public WorkflowData getWorkflowData() {
		return this.workflowData;
	}

	@JsonIgnore
	public void setWorkflowData(WorkflowData workflowData) {
		this.workflowData = workflowData;
	}

	public static State getState(String stateStr) {
		for (State state : State.values()) {
			if (stateStr.equalsIgnoreCase(state.name())) {
				return state;
			}
		}
		return null;
	}

	public enum State {
		CREATED, RUNNING, STOPPED, ERROR;
		static State toState(String v) {
			if (v == null) {
				return null;
			}
			for (State s : State.values()) {
				if (s.name().equalsIgnoreCase(v)) {
					return s;
				}
			}
			return null;
		}
	}
}
