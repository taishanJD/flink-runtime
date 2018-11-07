package com.quark.datastream.runtime.common.workflow;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.quark.datastream.runtime.common.Format;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowEditorMetadata extends Format {

	private static final long serialVersionUID = 3399185186662849550L;
	private Long workflowId;
	private String data;
	private Long timestamp;

	public WorkflowEditorMetadata() {

	}

	public Long getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(Long workflowId) {
		if (workflowId == null) {
			throw new RuntimeException("Invalid workflow id");
		}
		this.workflowId = workflowId;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}
