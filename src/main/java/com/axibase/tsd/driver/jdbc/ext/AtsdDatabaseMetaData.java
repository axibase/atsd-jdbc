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
package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.json.Version;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.util.DbMetadataUtils;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static com.axibase.tsd.driver.jdbc.DriverConstants.REVISION_LINE;

public class AtsdDatabaseMetaData extends AvaticaDatabaseMetaData {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdDatabaseMetaData.class);
	@Getter
	@Setter
	private String revision = "Unknown Revision";
	@Getter
	@Setter
	private String edition = "Unknown Edition";

	protected AtsdDatabaseMetaData(AvaticaConnection connection) {
		super(connection);
	}

	public void initVersions(AtsdConnectionInfo atsdConnectionInfo) throws SQLException {
		final String versionUrl = Location.VERSION_ENDPOINT.getUrl(atsdConnectionInfo);
		final ContentDescription contentDescription = new ContentDescription(versionUrl, atsdConnectionInfo);
		try (final IContentProtocol protocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription)) {
			assert protocol != null;
			final InputStream databaseInfo = protocol.readInfo();
			final Version version = JsonMappingUtil.mapToVersion(databaseInfo);
			if (logger.isTraceEnabled()) {
				logger.trace("[initVersions] " + version.toString());
			}
			edition = version.getLicense().getProductVersion();
			revision = version.getBuildInfo().getRevisionNumber();
			if (logger.isDebugEnabled()) {
				logger.debug("[initVersions] edition: " + edition);
				logger.debug("[initVersions] revision: " + revision);
			}
		} catch (UnknownHostException e) {
			if (logger.isDebugEnabled()) {
				logger.debug(e.getMessage());
			}
			throw new SQLException("Unknown host specified", e);
		} catch (final Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug(e.getMessage());
			}
			throw new SQLException(e);
		}
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getTables] catalog: {}, schemaPattern: {}, tableNamePattern: {}, types: {}",
					catalog, schemaPattern, tableNamePattern, Arrays.toString(types));
		}
		return super.getTables(catalog, schemaPattern, tableNamePattern, types);

	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getSchemas] catalog: {}, schemaPattern: {}", catalog, schemaPattern);
		}
		return super.getSchemas(catalog, schemaPattern);
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getSchemas]");
		}
		return super.getSchemas();
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getColumnPrivileges] catalog: {}, schema: {}, table: {}, columnNamePattern: {}",
					catalog, schema, table, columnNamePattern);
		}
		return super.getColumnPrivileges(catalog, schema, table, columnNamePattern);
	}

	public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, final String columnNamePattern) throws SQLException {
		logger.debug("[getColumns] catalog: {}, schemaPattern: {}, tableNamePattern: {}, columnNamePattern: {}",
					catalog, schemaPattern, tableNamePattern, columnNamePattern);
		return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}

		@Override
	public ResultSet getCatalogs() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getCatalogs]");
		}
		return super.getCatalogs();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getTableTypes]");
		}
		return super.getTableTypes();
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getTypeInfo]");
		}
		return super.getTypeInfo();
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		logger.debug("[getDatabaseProductName]");
		final String result = super.getDatabaseProductName();
		logger.trace("[getDatabaseProductName] name: {}", result);
		return result;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		logger.debug("[getDatabaseProductVersion]");
		final String result = String.format("%s, %s, %s: %s", super.getDatabaseProductVersion(), edition, REVISION_LINE, revision);
		logger.trace("[getDatabaseProductVersion] version: {}", result);
		return result;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return getConnectedAtsdVersion();
	}

	int getConnectedAtsdVersion() {
		try {
			return Integer.parseInt(revision);
		} catch (NumberFormatException e) {
			if (logger.isDebugEnabled()) {
				logger.debug(e.getMessage());
			}
		}
		return 1;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getSQLKeywords]");
		}
		return DbMetadataUtils.getSqlKeywords();
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getNumericFunctions]");
		}
		return DbMetadataUtils.getNumericFunctions();
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[supportsBatchUpdates]");
		}
		return true;
	}

	@Override
	public String getStringFunctions() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getStringFunctions]");
		}
		return DbMetadataUtils.getStringFunctions();
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getSystemFunctions]");
		}
		return super.getSystemFunctions();
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getTimeDateFunctions]");
		}
		return DbMetadataUtils.getSupportedTimeFunctions();
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return super.supportsDataManipulationTransactionsOnly();
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 128;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 256;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 65536;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 128;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 1024;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 128 * 1024;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "database";
	}

}
