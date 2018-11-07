package com.quark.datastream.runtime.topologygenerator.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.quark.datastream.runtime.common.workflow.WorkflowData.EngineType;

public final class EngineManager {

	private final Map<String, Engine> engineMap;

	private static EngineManager instance = null;

	public synchronized static EngineManager getInstance() {
		if (instance == null) {
			instance = new EngineManager();
		}
		return instance;
	}

	private EngineManager() {
		this.engineMap = new ConcurrentHashMap<>();
	}

	public synchronized Engine getEngine(String host, int port, EngineType engineType) {
		String key = getKey(host, port, engineType);
		Engine engine = engineMap.get(key);
		if (engine == null) {
			engine = EngineFactory.createEngine(engineType, host, port);
			engineMap.put(key, engine);
		}
		return engine;
	}

	private String getKey(String host, int port, EngineType engineType) {
		return String.format("%s_%s_%d", engineType.name(), host, port);
	}
}
