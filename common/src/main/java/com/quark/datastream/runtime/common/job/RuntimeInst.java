package com.quark.datastream.runtime.common.job;

import java.io.Serializable;
import java.util.Date;

/**
 * 网关与服务器通用返回类型
 * 
 * @author WangHao 2018年8月22日
 */
public class RuntimeInst implements Serializable {

	private static final long serialVersionUID = -8638540542514255555L;

	private Long id;

	private Long dataStreamWorkflowId;

	private Long runtimeEnvId;

	private String workflowInstName;

	private Date startTime;

	private Date endTime;

	private String status;

	private String runtimeJobId;

	private Integer total;

	private Integer created;

	private Integer scheduled;

	private Integer deploying;

	private Integer running;

	private Integer finished;

	private Integer canceling;

	private Integer canceled;

	private Integer failed;

	private Integer reconciling;

	private Long duration;

	private String isDel;

	private Date createTime;

	private Date updateTime;

	private String runtimeJson;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getDataStreamWorkflowId() {
		return dataStreamWorkflowId;
	}

	public void setDataStreamWorkflowId(Long dataStreamWorkflowId) {
		this.dataStreamWorkflowId = dataStreamWorkflowId;
	}

	public Long getRuntimeEnvId() {
		return runtimeEnvId;
	}

	public void setRuntimeEnvId(Long runtimeEnvId) {
		this.runtimeEnvId = runtimeEnvId;
	}

	public String getWorkflowInstName() {
		return workflowInstName;
	}

	public void setWorkflowInstName(String workflowInstName) {
		this.workflowInstName = workflowInstName;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getRuntimeJobId() {
		return runtimeJobId;
	}

	public void setRuntimeJobId(String runtimeJobId) {
		this.runtimeJobId = runtimeJobId;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public Integer getCreated() {
		return created;
	}

	public void setCreated(Integer created) {
		this.created = created;
	}

	public Integer getScheduled() {
		return scheduled;
	}

	public void setScheduled(Integer scheduled) {
		this.scheduled = scheduled;
	}

	public Integer getDeploying() {
		return deploying;
	}

	public void setDeploying(Integer deploying) {
		this.deploying = deploying;
	}

	public Integer getRunning() {
		return running;
	}

	public void setRunning(Integer running) {
		this.running = running;
	}

	public Integer getFinished() {
		return finished;
	}

	public void setFinished(Integer finished) {
		this.finished = finished;
	}

	public Integer getCanceling() {
		return canceling;
	}

	public void setCanceling(Integer canceling) {
		this.canceling = canceling;
	}

	public Integer getCanceled() {
		return canceled;
	}

	public void setCanceled(Integer canceled) {
		this.canceled = canceled;
	}

	public Integer getFailed() {
		return failed;
	}

	public void setFailed(Integer failed) {
		this.failed = failed;
	}

	public Integer getReconciling() {
		return reconciling;
	}

	public void setReconciling(Integer reconciling) {
		this.reconciling = reconciling;
	}

	public Long getDuration() {
		return duration;
	}

	public void setDuration(Long duration) {
		this.duration = duration;
	}

	public String getIsDel() {
		return isDel;
	}

	public void setIsDel(String isDel) {
		this.isDel = isDel;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public String getRuntimeJson() {
		return runtimeJson;
	}

	public void setRuntimeJson(String runtimeJson) {
		this.runtimeJson = runtimeJson;
	}

}
