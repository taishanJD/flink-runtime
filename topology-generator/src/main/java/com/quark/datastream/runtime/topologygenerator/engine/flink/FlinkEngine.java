package com.quark.datastream.runtime.topologygenerator.engine.flink;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.quark.datastream.runtime.common.Settings;
import com.quark.datastream.runtime.common.job.Job;
import com.quark.datastream.runtime.common.job.RuntimeInst;
import com.quark.datastream.runtime.common.utils.HTTP;
import com.quark.datastream.runtime.common.utils.RuntimeState;
import com.quark.datastream.runtime.common.workflow.WorkflowData;
import com.quark.datastream.runtime.common.workflow.WorkflowData.EngineType;
import com.quark.datastream.runtime.common.workflow.WorkflowProcessor;
import com.quark.datastream.runtime.topologygenerator.engine.AbstractEngine;
import org.springframework.util.CollectionUtils;

/**
 * flink流式引擎
 */
public class FlinkEngine extends AbstractEngine {

	private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEngine.class);
	private String defaultJobJarLocation = Settings.getInstance().getResourcePath();

	private String defaultLauncherJarLocation = Settings.getInstance().getResourcePath() + "engine-flink.jar";

	private HTTP httpClient = null;

	public FlinkEngine(String host, int port) {
		super(host, port);
		this.httpClient = new HTTP();
		this.httpClient.initialize(host, port, "http");
	}

	@Override
	public void create(Job job) throws Exception {
		WorkflowData workflowData = job.getWorkflowData();
		job.setConfig(workflowData.getConfig());
		job.setEngineType(EngineType.FLINK.name());
		job.setHost(this.host);
		job.setPort(this.port);
		List<Path> jobSpecificData = new ArrayList<>();
		if(!CollectionUtils.isEmpty(workflowData.getProcessors())) {
			jobSpecificData.addAll(getModelInfo(workflowData));
		}
		jobSpecificData.add(prepareFlinkJobPlan(workflowData, job.getJobId()));
		// 部署Jar
		Path jobJarFile = prepareJarToDeploy(jobSpecificData, job.getJobId());
		if (jobJarFile == null) {
			throw new RuntimeException("Failed to prepare jar file to deploy.");
		}
		String launcherJarId = uploadLauncherJar(jobJarFile);
		if (launcherJarId == null) {
			throw new RuntimeException("Failed to upload Flink jar; Please check out connection");
		}
		job.addConfig("launcherJarId", launcherJarId);
	}

	private String uploadLauncherJar(Path path) throws Exception {
		File jarFile = path.toFile();
		JsonElement jsonString = this.httpClient.post("/jars/upload", jarFile);
		if (jsonString == null) {
			return null;
		}
		JsonObject jsonResponse = jsonString.getAsJsonObject();
		JsonElement fileName = jsonResponse.get("filename");
		if (fileName != null) {
			String jarId = fileName.getAsString(); // TODO: Exception handling
			jarId = jarId.split("/")[4];
			return jarId;
		} else {
			LOGGER.warn("file name is null");
			return null;
		}
	}

	private Path prepareFlinkJobPlan(WorkflowData workflowData, String jobId) throws Exception {
		String jsonConfig = new Gson().toJson(workflowData);
		String targetPath = defaultJobJarLocation + jobId + ".json";
		Path configJson = Paths.get(targetPath);
		File configJsonFile = configJson.toFile();

		if (configJsonFile.exists()) {
			if (!configJsonFile.delete()) {
				throw new RuntimeException("Unable to delete old configuration " + configJson);
			}
		}

		FileUtils.writeStringToFile(configJsonFile, jsonConfig);

		return configJson.toAbsolutePath();
	}

	private Set<Path> getModelInfo(WorkflowData workflowData) {
		Set<Path> artifacts = new HashSet<>();
		for (WorkflowProcessor processor : workflowData.getProcessors()) {
			String className = processor.getClassname();
			processor.getConfig().getProperties().put("className", className);
			String jarPath = processor.getPath();
			Path artifact = Paths.get(jarPath);
			Path fileName = artifact.getFileName();
			if (fileName != null) {
				processor.getConfig().getProperties().put("jar", fileName.toString());
			} else {
				LOGGER.warn("fileName is null");
			}
			artifacts.add(artifact);
		}
		if (artifacts.size() == 0) {
			throw new IllegalStateException("At least one processor required");
		}
		return artifacts;
	}

	private Path prepareJarToDeploy(List<Path> jobData, String jarName) throws Exception {
		Path referenceJar = Paths.get(defaultLauncherJarLocation);
		Path targetJar = Paths.get(defaultJobJarLocation + jarName + ".jar");
		if (targetJar.toFile().exists()) {
			LOGGER.info("Delete old version of Job Jar file {}", targetJar.toAbsolutePath().toString());
			if (targetJar.toFile().delete() == false) {
				LOGGER.warn("Failed in attempt to delete old version of Job Jar file");
			}
		}
		Path newJar = Files.copy(referenceJar, targetJar);
		List<String> commands = new ArrayList<>();
		commands.add("jar");
		commands.add("uf");
		commands.add(newJar.toString());
		for (Path inputFile : jobData) {
			commands.add("-C");
			Path parent = inputFile.getParent();
			if (parent != null) {
				commands.add(parent.toString());
			} else {
				LOGGER.warn("getParent is null");
			}

			Path fileName = inputFile.getFileName();
			if (fileName != null) {
				commands.add(fileName.toString());
			} else {
				LOGGER.warn("getFileName is null");
			}
		}
		Process process = executeShellProcess(commands);
		ShellProcessResult shellProcessResult = waitProcessFor(process);
		if (shellProcessResult.exitValue != 0) {
			LOGGER.error("Adding job-specific data to jar is failed - exit code: {} / output: {}",
					shellProcessResult.exitValue, shellProcessResult.stdout);
			throw new RuntimeException(
					"Workflow could not be deployed " + "successfully: fail to add config and artifacts to jar");
		}
		LOGGER.info("Added files to jar {}", jarName);
		return targetJar;
	}

	private Process executeShellProcess(List<String> commands) throws Exception {
		LOGGER.info("Executing command: " + Joiner.on(" ").join(commands));
		ProcessBuilder processBuilder = new ProcessBuilder(commands);
		processBuilder.redirectErrorStream(true);
		return processBuilder.start();
	}

	private ShellProcessResult waitProcessFor(Process process) throws IOException, InterruptedException {
		StringWriter sw = new StringWriter();
		IOUtils.copy(process.getInputStream(), sw);
		String stdout = sw.toString();
		process.waitFor();
		int exitValue = process.exitValue();
		LOGGER.debug("Command output: " + stdout);
		LOGGER.debug("Command exit status: " + exitValue);
		return new ShellProcessResult(exitValue, stdout);
	}

	private static class ShellProcessResult {
		private final int exitValue;
		private final String stdout;

		ShellProcessResult(int exitValue, String stdout) {
			this.exitValue = exitValue;
			this.stdout = stdout;
		}
	}

	@Override
	public String run(Job job) throws Exception {
		if (job == null) {
			throw new NullPointerException("Job is null.");
		}
		if (job.getJobId() == null) {
			throw new IllegalStateException("Job id does not exist.");
		}
		String launcherJarId;
		if ((launcherJarId = job.getConfig("launcherJarId")) == null) {
			throw new IllegalStateException(
					"Launcher jar for job(" + job.getJobId() + ") does not exist. Make sure job is created first.");
		}
		Map<String, String> args = new HashMap<>();
		args.put("program-args", String.format("--internal --json %s", job.getJobId()));
		args.put("entry-class", "com.quark.datastream.runtime.engine.flink.Launcher");
		args.put("parallelism", "1");
		LOGGER.info("Running job {}({})", new Object[] { job.getJobId(), launcherJarId });
		JsonObject flinkResponse = this.httpClient.post("/jars/" + launcherJarId + "/run", args, true)
				.getAsJsonObject();
		LOGGER.debug("/run response: {}", flinkResponse);
		String jobId = flinkResponse.get("jobid").getAsString();
		if (jobId == null) {
			JsonElement error = flinkResponse.get("error");
			if (error != null) {
				throw new RuntimeException(error.getAsString());
			}
		}
		return jobId;
	}

	@Override
	public void stop(Job job) throws Exception {
		if (job == null) {
			throw new NullPointerException("Job is null.");
		}
		JsonElement flinkResponse = this.httpClient.get("/jobs/" + job.getEngineId() + "/yarn-cancel");
		LOGGER.debug("/jobs/{}/cancel response: {}", job.getEngineId(), flinkResponse);
	}

	@Override
	public void delete(Job job) throws Exception {
		// Flink engine not have this options
		throw new RuntimeException("Flink engine not have this options");
	}

	@Override
	public RuntimeInst detail(Job job) throws Exception {
		RuntimeInst runtimeInst = new RuntimeInst();
		runtimeInst.setRuntimeJobId(job.getEngineId());
		JsonElement flinkResponse = this.httpClient.get("/jobs/" + job.getEngineId());
		JsonObject asJsonObject = flinkResponse.getAsJsonObject();
		runtimeInst.setWorkflowInstName(asJsonObject.get("name").getAsString());
		runtimeInst.setStatus(RuntimeState.getKeyByValue(asJsonObject.get("state").getAsString()));
		Date startDate = new Date();
		startDate.setTime(asJsonObject.get("start-time").getAsLong());
		runtimeInst.setStartTime(startDate);
		Date endDate = new Date();
		startDate.setTime(asJsonObject.get("end-time").getAsLong());
		runtimeInst.setEndTime(endDate);
		return runtimeInst;
	}

	@Override
	public List<RuntimeInst> jobList() throws Exception {
		List<RuntimeInst> flinkJobList = new ArrayList<RuntimeInst>();
		JsonElement flinkResponse = this.httpClient.get("/jobs/overview");
		JsonObject jsonObject = flinkResponse.getAsJsonObject();
		JsonArray jobsJsonArray = jsonObject.getAsJsonArray("jobs");
		for (JsonElement jsonElement : jobsJsonArray) {
			JsonObject jobJsonObject = jsonElement.getAsJsonObject();
			RuntimeInst inst = new RuntimeInst();
			inst.setRuntimeJobId(jobJsonObject.get("jid").getAsString());
			inst.setWorkflowInstName(jobJsonObject.get("name").getAsString());
			inst.setStatus(RuntimeState.getKeyByValue(jobJsonObject.get("state").getAsString()));
			Date startDate = new Date();
			startDate.setTime(jobJsonObject.get("start-time").getAsLong());
			inst.setStartTime(startDate);
			Date endDate = new Date();
			startDate.setTime(jobJsonObject.get("end-time").getAsLong());
			inst.setEndTime(endDate);
			inst.setDuration(jobJsonObject.get("duration").getAsLong());
			Date updateDate = new Date();
			updateDate.setTime(jobJsonObject.get("last-modification").getAsLong());
			inst.setUpdateTime(updateDate);
			JsonObject taskJsonObject = jobJsonObject.get("tasks").getAsJsonObject();
			inst.setTotal(taskJsonObject.get("total").getAsInt());
			inst.setCreated(taskJsonObject.get("created").getAsInt());
			inst.setScheduled(taskJsonObject.get("scheduled").getAsInt());
			inst.setDeploying(taskJsonObject.get("deploying").getAsInt());
			inst.setRunning(taskJsonObject.get("running").getAsInt());
			inst.setFinished(taskJsonObject.get("finished").getAsInt());
			inst.setCanceling(taskJsonObject.get("canceling").getAsInt());
			inst.setCanceled(taskJsonObject.get("canceled").getAsInt());
			inst.setFailed(taskJsonObject.get("failed").getAsInt());
			inst.setReconciling(taskJsonObject.get("reconciling").getAsInt());
			flinkJobList.add(inst);
		}
		return flinkJobList;
	}
}
