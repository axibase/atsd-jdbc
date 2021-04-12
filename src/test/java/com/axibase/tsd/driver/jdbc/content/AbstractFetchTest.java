package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.TestUtil;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import org.apache.calcite.avatica.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;

import static com.axibase.tsd.driver.jdbc.AtsdProperties.READ_STRATEGY;
import static org.junit.Assert.assertEquals;

public abstract class AbstractFetchTest {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTypeMock.class);

	protected void fetch(final IStoreStrategy storeStrategy, String resource, int fetchSize) throws AtsdException, SQLException {
		long start = System.currentTimeMillis();
		final Class<?> thisClass = getClass();
		try (final InputStream mockIs = TestUtil.getInputStreamForResource(resource, thisClass)) {
			storeStrategy.store(mockIs);
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
			assertEquals(fetched.size(), fetchSize);
		} catch (final IOException e) {
			logger.error(e.getMessage(), e);
			throw new AtsdException(e.getMessage());
		} finally {
			if (logger.isDebugEnabled()) {
				logger.debug("Test [ContentProvider->fetch] is done in " + (System.currentTimeMillis() - start) + " msecs");
			}
		}
	}

	protected static IStoreStrategy getStrategyObject() {
		final Meta.StatementHandle statementHandle = new Meta.StatementHandle("12345678", 1, null);
		final StatementContext context = new StatementContext(statementHandle, false);
		return Strategy.byName(READ_STRATEGY).initialize(context, OnMissingMetricAction.ERROR);
	}
}
