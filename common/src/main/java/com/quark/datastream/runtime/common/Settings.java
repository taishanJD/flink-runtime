package com.quark.datastream.runtime.common;

public final class Settings {

	public static final String DOCKER_PATH = "/runtime/data/";
	public static final String FW_JAR_PATH = DOCKER_PATH + "jar/task/";
	public static final String CUSTOM_JAR_PATH = DOCKER_PATH + "jar/task_user/";
	public static final String RESOURCE_PATH = DOCKER_PATH + "resource/";

	private static Settings instance = null;

	private Settings() {

	}

	public synchronized static Settings getInstance() {
		if (instance == null) {
			instance = new Settings();
		}
		return instance;
	}

	public String getDockerPath() {
		return DOCKER_PATH;
	}

	public String getFwJarPath() {
		return FW_JAR_PATH;
	}

	public String getCustomJarPath() {
		return CUSTOM_JAR_PATH;
	}

	public String getResourcePath() {
		return RESOURCE_PATH;
	}
}
