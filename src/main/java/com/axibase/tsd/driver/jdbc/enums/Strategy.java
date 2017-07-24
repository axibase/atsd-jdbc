package com.axibase.tsd.driver.jdbc.enums;


import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.strategies.memory.MemoryStrategy;
import com.axibase.tsd.driver.jdbc.strategies.storage.FileStoreStrategy;
import com.axibase.tsd.driver.jdbc.strategies.stream.StreamStrategy;
import lombok.Getter;

@Getter
public enum Strategy {
	FILE(FileStoreStrategy.class, "File"),
	STREAM(StreamStrategy.class, "Stream"),
	MEMORY(MemoryStrategy.class, "Stream");

	private final Class<? extends IStoreStrategy> strategyClass;
	private final String source;


	Strategy(Class<? extends IStoreStrategy> strategyClass, String source) {
		this.strategyClass = strategyClass;
		this.source = source;
	}
}
