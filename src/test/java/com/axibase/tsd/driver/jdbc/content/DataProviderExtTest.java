/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataProvider.class)
public class DataProviderExtTest extends AbstractFetchTest {
	private static final Logger logger = LoggerFactory.getLogger(DataProviderExtTest.class);

	@Before
	public void setUp() throws Exception {
		protocolImpl = PowerMockito.mock(SdkProtocolImpl.class);
	}

	@Test
	public void testStrategyOnOne() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/1.csv", 1);
		}
	}

	@Test
	public void testStrategyStrategyOn143() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/143.csv", 143);
		}
	}

	@Test
	public void testStrategyOn20001() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/20001.csv", 20001);
		}
	}

	@Test
	public void testStrategyOnTags() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/tags.csv", 6);
		}
	}

	@Test(expected = AtsdRuntimeException.class)
	public void testStrategyOnSqleWithoutRecords() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/docker.network.eth0.rxerrors.csv", 1);
		}
	}

	@Test(expected = AtsdRuntimeException.class)
	public void testStrategyOnSqleWithRecords() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/df.bytes.free.csv.zip", 18835);
		}
	}

	@Test(expected = AtsdRuntimeException.class)
	public void testStrategyOnSqleWithManyRecords() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, "/csv/gc_time_persent.csv.zip", 323115);
		}
	}
}
