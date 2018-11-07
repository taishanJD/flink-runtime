package com.quark.datastream.runtime.topologygenerator.engine;

import com.quark.datastream.runtime.common.workflow.WorkflowData;
import com.quark.datastream.runtime.topologygenerator.engine.batch.BatchEngine;
import com.quark.datastream.runtime.topologygenerator.engine.flink.FlinkEngine;
import com.quark.datastream.runtime.topologygenerator.engine.instant.InstantEngine;

public class EngineFactory {

	public static Engine createEngine(WorkflowData.EngineType engineType, String host, Integer port) {
		final Engine engine;
		switch (engineType) {
		case FLINK:
			engine = new FlinkEngine(host, port);
			break;
		case BATCH:
			engine = new BatchEngine(host, port);
			break;
		case INSTANT:
			engine = new InstantEngine(host, port);
			break;
		default:
			throw new RuntimeException("Unsupported engine type selected.");
		}
		return engine;
	}
}
