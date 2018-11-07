package com.quark.datastream.runtime.common.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.quark.datastream.runtime.common.Format;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowEdge extends Format {

	private static final long serialVersionUID = -668856709351402237L;
	private Long id;
	private Long workflowId;
	private Long fromId;
	private Long toId;
	private List<StreamGrouping> streamGroupings = new ArrayList<>();

	public WorkflowEdge() {

	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(Long workflowId) {
		this.workflowId = workflowId;
	}

	public Long getFromId() {
		return fromId;
	}

	public void setFromId(Long fromId) {
		this.fromId = fromId;
	}

	public Long getToId() {
		return toId;
	}

	public void setToId(Long toId) {
		this.toId = toId;
	}

	public List<StreamGrouping> getStreamGroupings() {
		return streamGroupings;
	}

	public void setStreamGroupings(List<StreamGrouping> streamGroupings) {
		if (streamGroupings == null) {
			throw new RuntimeException("Invalid stream groupings");
		}
		this.streamGroupings = streamGroupings;
	}

	@JsonIgnore
	public String getStreamGroupingsStr() {
		if (this.streamGroupings.isEmpty()) {
			return "[]";
		} else {
			try {
				return mapper.writeValueAsString(this.streamGroupings);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@JsonIgnore
	public void setStreamGroupingsStr(String groupings) {
		try {
			if (StringUtils.isEmpty(groupings)) {
				throw new RuntimeException("Invalid groupings");
			}
			this.streamGroupings = mapper.readValue(groupings, new TypeReference<List<StreamGrouping>>() {
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@JsonInclude(Include.NON_NULL)
	public static class StreamGrouping extends Format {
		private static final long serialVersionUID = -1189096134370882021L;
		private Long streamId;
		private Grouping grouping;
		private List<String> fields;

		public Long getStreamId() {
			return streamId;
		}

		public void setStreamId(Long streamId) {
			this.streamId = streamId;
		}

		public Grouping getGrouping() {
			return grouping;
		}

		public void setGrouping(Grouping grouping) {
			this.grouping = grouping;
		}

		public List<String> getFields() {
			return fields;
		}

		public void setFields(List<String> fields) {
			this.fields = fields;
		}
	}

	public enum Grouping {
		SHUFFLE, FIELDS
	}
}
