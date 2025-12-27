package com.bedatadriven.hrana;

import com.bedatadriven.hrana.proto.Hrana;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

class HranaPreparedStatement implements PreparedStatement {
  private final HranaHttpStream stream;
  private final String sql;

  private final List<Object> params = new ArrayList<>();
  private HranaResultSet currentResultSet;
  private int updateCount = -1;
  private boolean closed = false;

  public HranaPreparedStatement(HranaHttpStream stream, String sql) {
    this.stream = stream;
    this.sql = sql;
  }

  private void ensureOpen() throws SQLException {
    if (closed) {
      throw new SQLException("PreparedStatement is closed");
    }
  }

  private void setParam(int index, Object value) throws SQLException {
    if (index < 1) {
      throw new SQLException("Parameter index starts at 1");
    }
    while (params.size() < index) {
      params.add(null);
    }
    params.set(index - 1, value);
  }

  private Hrana.StmtResult executeInternal() throws SQLException {
    try {
      return stream.executePrepared(sql, params);
    } catch (IOException e) {
      throw new SQLException("I/O error executing prepared SQL", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while executing prepared SQL", e);
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal();
    if (result.getColsCount() == 0) {
      throw new SQLException("Query did not produce a result set");
    }
    this.currentResultSet = new HranaResultSet(result);
    this.updateCount = -1;
    return this.currentResultSet;
  }

  @Override
  public int executeUpdate() throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal();
    this.currentResultSet = null;
    long count = result.getAffectedRowCount();
    this.updateCount = (count > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    return this.updateCount;
  }

  @Override
  public boolean execute() throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal();
    boolean returnsResultSet = result.getColsCount() > 0;
    if (returnsResultSet) {
      this.currentResultSet = new HranaResultSet(result);
      this.updateCount = -1;
      return true;
    } else {
      this.currentResultSet = null;
      long count = result.getAffectedRowCount();
      this.updateCount = (count > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    currentResultSet = null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxFieldSize");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("setMaxFieldSize");
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxRows");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("setMaxRows");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException("setEscapeProcessing");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("getQueryTimeout");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("setQueryTimeout");
  }

  @Override
  public void cancel() throws SQLException {
    throw new SQLFeatureNotSupportedException("cancel");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // no-op
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("setCursorName");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return currentResultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return updateCount;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    this.currentResultSet = null;
    this.updateCount = -1;
    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    this.currentResultSet = null;
    this.updateCount = -1;
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // ignored
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("addBatch");
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    // Not applicable for PreparedStatement
    throw new SQLFeatureNotSupportedException("addBatch(sql)");
  }

  @Override
  public void clearParameters() throws SQLException {
    params.clear();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    setParam(parameterIndex, null);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    setParam(parameterIndex, (long) x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    setParam(parameterIndex, (long) x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    setParam(parameterIndex, (double) x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, java.math.BigDecimal x) throws SQLException {
    if (x == null) {
      setParam(parameterIndex, null);
    } else {
      setParam(parameterIndex, x.doubleValue());
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    setParam(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
    setParam(parameterIndex, x == null ? null : x.getTime());
  }

  @Override
  public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
    setParam(parameterIndex, x == null ? null : x.getTime());
  }

  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
    setParam(parameterIndex, x == null ? null : x.getTime());
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    setParam(parameterIndex, x);
  }

  // Unsupported setter variants below
  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("setAsciiStream"); }
  @Override
  public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("setUnicodeStream"); }
  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException("setBinaryStream"); }
  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) throws SQLException { throw new SQLFeatureNotSupportedException("setCharacterStream"); }
  @Override
  public void clearBatch() throws SQLException { throw new SQLFeatureNotSupportedException("clearBatch"); }
  @Override
  public int[] executeBatch() throws SQLException { throw new SQLFeatureNotSupportedException("executeBatch"); }
  @Override
  public ResultSetMetaData getMetaData() throws SQLException { throw new SQLFeatureNotSupportedException("getMetaData"); }
  @Override
  public void setURL(int parameterIndex, java.net.URL x) throws SQLException { throw new SQLFeatureNotSupportedException("setURL"); }
  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException { throw new SQLFeatureNotSupportedException("getParameterMetaData"); }
  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException("setRowId"); }
  @Override
  public void setNString(int parameterIndex, String value) throws SQLException { setString(parameterIndex, value); }
  @Override
  public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setNCharacterStream"); }
  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException { throw new SQLFeatureNotSupportedException("setNClob"); }
  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException("setClob"); }
  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException("setBlob"); }
  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException { throw new SQLFeatureNotSupportedException("setArray"); }
  @Override
  public void setDate(int parameterIndex, java.sql.Date x, java.util.Calendar cal) throws SQLException { setDate(parameterIndex, x); }
  @Override
  public void setTime(int parameterIndex, java.sql.Time x, java.util.Calendar cal) throws SQLException { setTime(parameterIndex, x); }
  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal) throws SQLException { setTimestamp(parameterIndex, x); }
  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException { setNull(parameterIndex, sqlType); }
  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException("setRef"); }
  @Override
  public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setBlob"); }
  @Override
  public void setBlob(int parameterIndex, java.io.InputStream inputStream) throws SQLException { throw new SQLFeatureNotSupportedException("setBlob"); }
  @Override
  public void setClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setClob"); }
  @Override
  public void setClob(int parameterIndex, java.io.Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("setClob"); }
  @Override
  public void setNClob(int parameterIndex, java.io.Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setNClob"); }
  @Override
  public void setNClob(int parameterIndex, java.io.Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("setNClob"); }
  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException { throw new SQLFeatureNotSupportedException("setSQLXML"); }
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException { setObject(parameterIndex, x); }
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException { setObject(parameterIndex, x); }
  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setAsciiStream"); }
  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setBinaryStream"); }
  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) throws SQLException { throw new SQLFeatureNotSupportedException("setCharacterStream"); }
  @Override
  public void setAsciiStream(int parameterIndex, java.io.InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("setAsciiStream"); }
  @Override
  public void setBinaryStream(int parameterIndex, java.io.InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException("setBinaryStream"); }
  @Override
  public void setCharacterStream(int parameterIndex, java.io.Reader reader) throws SQLException { throw new SQLFeatureNotSupportedException("setCharacterStream"); }
  @Override
  public void setNCharacterStream(int parameterIndex, java.io.Reader value) throws SQLException { throw new SQLFeatureNotSupportedException("setNCharacterStream"); }

  // Statement interface delegated/implemented pieces
  @Override
  public ResultSet getGeneratedKeys() throws SQLException { throw new SQLFeatureNotSupportedException("getGeneratedKeys"); }

  @Override
  public int executeUpdate(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("executeUpdate(sql)"); }
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException("executeUpdate(sql,int)"); }
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException("executeUpdate(sql,int[])"); }
  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException("executeUpdate(sql,String[])"); }
  @Override
  public ResultSet executeQuery(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("executeQuery(sql)"); }
  @Override
  public boolean execute(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("execute(sql)"); }
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException("execute(sql,int)"); }
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException("execute(sql,int[])"); }
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException("execute(sql,String[])"); }

  @Override
  public int getResultSetHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
  @Override
  public boolean isClosed() throws SQLException { return closed; }
  @Override
  public void setPoolable(boolean poolable) throws SQLException { }
  @Override
  public boolean isPoolable() throws SQLException { return false; }
  @Override
  public void closeOnCompletion() throws SQLException { throw new SQLFeatureNotSupportedException("closeOnCompletion"); }
  @Override
  public boolean isCloseOnCompletion() throws SQLException { return false; }
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) return iface.cast(this);
    throw new SQLFeatureNotSupportedException("unwrap");
  }
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException { return iface.isInstance(this); }

  @Override
  public Connection getConnection() throws SQLException { throw new SQLFeatureNotSupportedException("getConnection"); }
}
