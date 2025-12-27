package com.bedatadriven.hrana;

import com.bedatadriven.hrana.proto.Hrana;

import java.io.IOException;
import java.sql.*;
import java.util.Collections;

/**
 * A JDBC Statement implementation over the Hrana libsql protocol.
 */
class HranaStatement implements Statement {

  private final HranaHttpStream stream;

  private HranaResultSet currentResultSet;
  private int updateCount = -1;
  private boolean closed = false;

  public HranaStatement(HranaHttpStream stream) {
    this.stream = stream;
  }

  private void ensureOpen() throws SQLException {
    if (closed) {
      throw new SQLException("Statement is closed");
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal(sql);
    // SELECT-like statements should return columns; if not, JDBC requires exception
    if (result.getColsCount() == 0) {
      throw new SQLException("Query did not produce a result set");
    }
    this.currentResultSet = new HranaResultSet(result);
    this.updateCount = -1;
    return this.currentResultSet;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal(sql);
    this.currentResultSet = null;
    long count = result.getAffectedRowCount();
    this.updateCount = (count > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    return this.updateCount;
  }

  private Hrana.StmtResult executeInternal(String sql) throws SQLException {
    try {
      // Use positional-params variant to get StmtResult back
      return stream.executePrepared(sql, Collections.emptyList());
    } catch (IOException e) {
      throw new SQLException("I/O error executing SQL", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while executing SQL", e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (closed) {
      return;
    }
    // Statement close does not close the underlying stream; the connection manages its lifecycle
    closed = true;
    currentResultSet = null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxFieldSize");
  }

  @Override
  public void setMaxFieldSize(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("setMaxFieldSize");
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxRows");
  }

  @Override
  public void setMaxRows(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("setMaxRows");
  }

  @Override
  public void setEscapeProcessing(boolean b) throws SQLException {
    // No-op; not supported
    throw new SQLFeatureNotSupportedException("setEscapeProcessing");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("getQueryTimeout");
  }

  @Override
  public void setQueryTimeout(int i) throws SQLException {
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
    // no warnings maintained
  }

  @Override
  public void setCursorName(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException("setCursorName");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    ensureOpen();
    Hrana.StmtResult result = executeInternal(sql);
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
  public ResultSet getResultSet() throws SQLException {
    return currentResultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return updateCount;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // Only single results supported right now
    this.currentResultSet = null;
    this.updateCount = -1;
    return false;
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    // Ignored; cursor is forward-only in this implementation
    if (i != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int i) throws SQLException {
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
  public void addBatch(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException("addBatch");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("clearBatch");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("executeBatch");
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new SQLFeatureNotSupportedException("getConnection");
  }

  @Override
  public boolean getMoreResults(int i) throws SQLException {
    // Only single results supported
    this.currentResultSet = null;
    this.updateCount = -1;
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("getGeneratedKeys");
  }

  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    // ignore auto-generated keys flag
    return executeUpdate(s);
  }

  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    return executeUpdate(s);
  }

  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    return executeUpdate(s);
  }

  @Override
  public boolean execute(String s, int i) throws SQLException {
    return execute(s);
  }

  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    return execute(s);
  }

  @Override
  public boolean execute(String s, String[] strings) throws SQLException {
    return execute(s);
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void setPoolable(boolean b) throws SQLException {
    // no pooling support in this simple implementation
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // not supported
    throw new SQLFeatureNotSupportedException("closeOnCompletion");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    throw new SQLFeatureNotSupportedException("unwrap");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return aClass.isInstance(this);
  }
}
