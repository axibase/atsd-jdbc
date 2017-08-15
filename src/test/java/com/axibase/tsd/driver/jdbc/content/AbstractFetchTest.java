package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.TestUtil;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.strategies.StrategyFactory;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.List;

import static com.axibase.tsd.driver.jdbc.AtsdProperties.READ_STRATEGY;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;

public abstract class AbstractFetchTest {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTypeMock.class);
	protected SdkProtocolImpl protocolImpl;

	protected void fetch(final IStoreStrategy storeStrategy, String resource, int fetchSize)
			throws AtsdException, IOException, GeneralSecurityException, SQLException {
		long start = System.currentTimeMillis();
		final Class<?> thisClass = getClass();
		final InputStream mockIs = TestUtil.getInputStreamForResource(resource, thisClass);
		try {
			PowerMockito.doReturn(mockIs).when(protocolImpl, "readContent", anyInt());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new AtsdException(e.getMessage());
		}
		try {
			final InputStream inputStream = protocolImpl.readContent(0);
			storeStrategy.store(inputStream);
			storeStrategy.openToRead(TestUtil.prepareMetadata(resource, thisClass));
			final List<List<Object>> fetched = storeStrategy.fetch(0L, fetchSize);
			final StatementContext context = storeStrategy.getContext();
			final SQLException exception = context.getException();
			if (exception != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("SQLException: " + exception.getMessage());
				}
				throw exception;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Fetched: " + fetched.size());
			}
			assertTrue(fetched.size() == fetchSize);
		} catch (final IOException e) {
			logger.error(e.getMessage(), e);
			throw new AtsdException(e.getMessage());
		} finally {
			if (mockIs != null) {
				mockIs.close();
			}
			if (logger.isDebugEnabled()) {
				logger.debug(
						"Test [ContentProvider->fetch] is done in " + (System.currentTimeMillis() - start) + " msecs");
			}
		}
	}

	protected static IStoreStrategy getMockStrategyObject() {
		final StatementContext context = new StatementContext();
		return PowerMockito.spy(StrategyFactory.create(StrategyFactory.findClassByName(READ_STRATEGY), context, OnMissingMetricAction.ERROR));
	}
}
