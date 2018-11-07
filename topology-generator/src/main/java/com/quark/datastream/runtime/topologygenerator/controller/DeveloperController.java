package com.quark.datastream.runtime.topologygenerator.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.quark.datastream.runtime.common.job.Job;
import com.quark.datastream.runtime.common.job.RuntimeInst;
import com.quark.datastream.runtime.common.utils.ResponseBody;
import com.quark.datastream.runtime.common.workflow.WorkFlowModel;
import com.quark.datastream.runtime.common.workflow.WorkflowData;
import com.quark.datastream.runtime.topologygenerator.controller.base.BaseController;
import com.quark.datastream.runtime.topologygenerator.engine.Engine;
import com.quark.datastream.runtime.topologygenerator.engine.EngineManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping("/developer")
@Api(value = "DeveloperController", tags = { "Developer Api" })
public class DeveloperController extends BaseController {

	private EngineManager engineManager = null;

	private static final Logger LOGGER = LoggerFactory.getLogger(DeveloperController.class);

	public DeveloperController() {
		this.engineManager = EngineManager.getInstance();
	}

	@RequestMapping(value = "/run", method = RequestMethod.POST)
	@ApiOperation(value = "运行工作流", notes = "将传入的工作流信息发送至对应的引擎中")
	public ResponseBody run(
			@RequestBody @ApiParam(name = "workFlowJson", value = "{}", required = true) String workFlowJson) {
		LOGGER.info("======== begin /run api ======== ");
		LOGGER.info("======== /run api param = {}", workFlowJson);
		WorkflowData workflowData;
		Job newJob;
		try {
			// 将传来的数据转换为对象
			workflowData = getMapper().readValue(workFlowJson, WorkflowData.class);
			// TODO: 2018/08/13 拆分json，分解为多个工作流
			// 创建引擎使用的Job对象
			newJob = Job.create(workflowData);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Server Error", e);
			return getErrorResponse();
		}
		// 创建引擎实例
		String targetHost = (String) workflowData.getConfig().get("targetHost");
		Engine engine = getEngine(targetHost, workflowData.getEngineType());
		RuntimeInst detail = null;
		try {
			engine.create(newJob);
			String engineId = engine.run(newJob);
			newJob.setEngineId(engineId);
			detail = engine.detail(newJob);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Server Error", e);
			return getErrorResponse();
		}
		return getSuccessResponse(detail);
	}

	@RequestMapping(value = "/stop", method = RequestMethod.POST)
	@ApiOperation(value = "停止工作流", notes = "将对应引擎中的工作流停止")
	public ResponseBody stop(
			@RequestBody @ApiParam(name = "WorkFlowModelJson", value = "{host,engineType,engineId}", required = true) String workFlowModelJson) {
		LOGGER.info("======== begin topology /stop api ======== ");
		LOGGER.info("======== /stop api param = {}", workFlowModelJson);
		try {
			// 将传来的数据转换为对象
			WorkFlowModel workFlowModel = getMapper().readValue(workFlowModelJson, WorkFlowModel.class);
			String host = workFlowModel.getHost();
			Engine engine = getEngine(host, WorkflowData.getEngineType(workFlowModel.getEngineType()));
			Job job = Job.create(workFlowModel.getEngineId());
			engine.stop(job);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Server Error", e);
			return getErrorResponse();
		}
		return getSuccessResponse();
	}

	@RequestMapping(value = "/jobList", method = RequestMethod.POST)
	@ApiOperation(value = "工作流列表", notes = "获取对应引擎中工作流列表")
	public ResponseBody jobList(
			@RequestBody @ApiParam(name = "WorkFlowModelJson", value = "{host,engineType}", required = true) String workFlowModelJson) {
		LOGGER.info("======== begin topology /jobList api ======== ");
		LOGGER.info("======== /jobList api param = {}", workFlowModelJson);
		try {
			// 将传来的数据转换为对象
			WorkFlowModel workFlowModel = getMapper().readValue(workFlowModelJson, WorkFlowModel.class);
			String host = workFlowModel.getHost();
			Engine engine = getEngine(host, WorkflowData.getEngineType(workFlowModel.getEngineType()));
			List<RuntimeInst> flinkJob = engine.jobList();
			return getSuccessResponse(flinkJob);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Server Error", e);
			// 该接口无参数，如果出现异常这说明IP错误或服务器本身异常
			return getResponse(404, null, "Connection refused");
		}
	}

	@RequestMapping(value = "/jobDetail", method = RequestMethod.POST)
	@ApiOperation(value = "获取任务详情", notes = "获取对应引擎中任务的详情")
	public ResponseBody jobDetail(
			@RequestBody @ApiParam(name = "WorkFlowModelJson", value = "{host,engineType,engineId}", required = true) String workFlowModelJson) {
		LOGGER.info("======== begin topology /jobDetail api ======== ");
		LOGGER.info("======== /jobDetail api param = {}", workFlowModelJson);
		try {
			// 将传来的数据转换为对象
			WorkFlowModel workFlowModel = getMapper().readValue(workFlowModelJson, WorkFlowModel.class);
			String host = workFlowModel.getHost();
			Engine engine = getEngine(host, WorkflowData.getEngineType(workFlowModel.getEngineType()));
			Job job = Job.create(workFlowModel.getEngineId());
			RuntimeInst detail = engine.detail(job);
			return getSuccessResponse(detail);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Server Error", e);
			if (e.getMessage().contains("404")) {
				return getResponse(404, null, "can not find this jobid");
			}
			return getErrorResponse();
		}
	}

	/**
	 * 获取引擎实例方法
	 */
	protected Engine getEngine(String targetHost, WorkflowData.EngineType engineType) {
		String[] splits = targetHost.split(":");
		return this.engineManager.getEngine(splits[0], Integer.parseInt(splits[1]), engineType);
	}
}
