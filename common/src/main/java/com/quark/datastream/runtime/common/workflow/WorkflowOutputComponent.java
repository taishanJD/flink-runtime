package com.quark.datastream.runtime.common.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;

public abstract class WorkflowOutputComponent extends WorkflowComponent {

	private static final long serialVersionUID = 591858092440539455L;
	private List<WorkflowStream> outputStreams = new ArrayList<>();

	public WorkflowOutputComponent() {

	}

	public List<WorkflowStream> getOutputStreams() {
		return outputStreams;
	}

	public void setOutputStreams(List<WorkflowStream> outputStreams) {
		if (outputStreams == null) {
			throw new RuntimeException("Invalid output streams");
		}
		this.outputStreams = outputStreams;
	}

	@JsonIgnore
	public void addOutputStream(WorkflowStream stream) {
		if (stream == null) {
			throw new RuntimeException("Invalid stream");
		}
		outputStreams.add(stream);
	}
}
