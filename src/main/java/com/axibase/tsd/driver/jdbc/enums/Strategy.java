package com.axibase.tsd.driver.jdbc.enums;


import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.strategies.memory.MemoryStrategy;
import com.axibase.tsd.driver.jdbc.strategies.storage.FileStoreStrategy;
import com.axibase.tsd.driver.jdbc.strategies.stream.StreamStrategy;
import lombok.Getter;

import java.util.Map;
import java.util.TreeMap;

@Getter
public enum Strategy {
	FILE("File") {
		@Override
		public IStoreStrategy initialize(StatementContext context, OnMissingMetricAction action) {
			return new FileStoreStrategy(context, action);
		}
	},
	STREAM("Stream") {
		@Override
		public IStoreStrategy initialize(StatementContext context, OnMissingMetricAction action) {
			return new StreamStrategy(context, action);
		}
	},
	MEMORY("Stream") {
		@Override
		public IStoreStrategy initialize(StatementContext context, OnMissingMetricAction action) {
			return new MemoryStrategy(context, action);
		}
	};

	private final String source;

	Strategy(String source) {
		this.source = source;
	}

	private static final Map<String, Strategy> strategyMap = createStrategyMap();

	public static Strategy byName(String name) {
		final Strategy strategy = strategyMap.get(name);
		if (strategy == null) {
			throw new IllegalStateException("Could not resolve strategy by name: " + name);
		}
		return strategy;
	}

	private static Map<String, Strategy> createStrategyMap() {
		Map<String, Strategy> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		for (Strategy strategy : Strategy.values()) {
			result.put(strategy.name(), strategy);
		}
		return result;
	}

	public abstract IStoreStrategy initialize(StatementContext context, OnMissingMetricAction action);
}
