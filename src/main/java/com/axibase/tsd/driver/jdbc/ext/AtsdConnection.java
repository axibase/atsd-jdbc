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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

import com.axibase.tsd.driver.jdbc.util.ExceptionsUtil;
import org.apache.calcite.avatica.*;

public class AtsdConnection extends AvaticaConnection {
	protected static final Trojan TROJAN = createTrojan();
	
	protected AtsdConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
		super(driver, factory, url, info);
	}

	@Override
	public AvaticaStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY ResultSet type is supported");
		}
		return super.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return true;
	}

	AtsdMeta getMeta(){
		return (AtsdMeta) TROJAN.getMeta(this);
	}

	@Override
	public void close() throws SQLException {
		super.close();
	}

	@Override
	protected ResultSet executeQueryInternal(AvaticaStatement statement, Meta.Signature signature, Meta.Frame firstFrame, QueryState state, boolean isUpdate) throws SQLException {
		try {
			return super.executeQueryInternal(statement, signature, firstFrame, state, isUpdate);
		} catch (SQLException e) {
			throw ExceptionsUtil.unboxException(e);
		}
	}

	AtsdConnectionInfo getConnectionInfo() {
		return new AtsdConnectionInfo(this.info);
	}

    AtsdDatabaseMetaData getAtsdDatabaseMetaData() throws SQLException {
        return (AtsdDatabaseMetaData) super.getMetaData();
    }

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		sql = StringUtils.stripStart(sql, null);
		return super.prepareStatement(sql);
	}

	@Override
	protected long[] executeBatchUpdateInternal(AvaticaPreparedStatement pstmt) throws SQLException {
		try {
			return super.executeBatchUpdateInternal(pstmt);
		} catch (SQLException e) {
			throw ExceptionsUtil.unboxException(e);
		}
	}

	AvaticaFactory getFactory() {
		return factory;
	}

}