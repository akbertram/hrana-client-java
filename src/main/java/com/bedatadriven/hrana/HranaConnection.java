package com.bedatadriven.hrana;

import java.io.IOException;
import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class HranaConnection implements Connection {
  private final HranaHttpStream stream;
  private boolean closed;
  private boolean autoCommit = true;

  public HranaConnection(HranaHttpStream stream) {
    this.stream = stream;
  }

  private void ensureOpen() throws SQLException {
    if (closed) {
      throw new SQLException("Connection is closed");
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    ensureOpen();
    return new HranaStatement(stream);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    ensureOpen();
    return new HranaPreparedStatement(stream, sql);
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    // No native SQL transformation is needed for SQLite/Hrana
    return s;
  }

  @Override
  public void setAutoCommit(boolean newAutoCommit) throws SQLException {
    ensureOpen();
    if (this.autoCommit == newAutoCommit) {
      return;
    }
    // Changing autocommit from false->true should commit current transaction per JDBC spec
    if (newAutoCommit) {
      try {
        stream.executePrepared("COMMIT", Collections.emptyList());
      } catch (IOException e) {
        throw new SQLException("I/O error during COMMIT", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted during COMMIT", e);
      }
      this.autoCommit = true;
    } else {
      // Switch to manual commit; start an explicit transaction to match server behavior
      try {
        stream.executeStatement("BEGIN");
      } catch (IOException e) {
        throw new SQLException("I/O error during BEGIN", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted during BEGIN", e);
      }
      this.autoCommit = false;
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    ensureOpen();
    try {
      boolean isAuto = stream.getAutocommit();
      this.autoCommit = isAuto;
      return isAuto;
    } catch (IOException e) {
      throw new SQLException("I/O error while fetching autocommit state", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while fetching autocommit state", e);
    }
  }

  @Override
  public void commit() throws SQLException {
    ensureOpen();
    if (autoCommit) {
      throw new SQLException("Cannot call commit() in auto-commit mode");
    }
    try {
      stream.executeStatements("COMMIT", "BEGIN");
    } catch (IOException e) {
      throw new SQLException("I/O error during COMMIT", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted during COMMIT", e);
    }
  }

  @Override
  public void rollback() throws SQLException {
    ensureOpen();
    if (autoCommit) {
      throw new SQLException("Cannot call rollback() in auto-commit mode");
    }
    try {
      stream.executeStatements("ROLLBACK", "BEGIN");
    } catch (IOException e) {
      throw new SQLException("I/O error during ROLLBACK", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted during ROLLBACK", e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (closed) {
      return;
    }
    this.closed = true;
    try {
      stream.close();
    } catch (IOException e) {
      throw new SQLException("I/O error closing stream", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while closing stream", e);
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException("DatabaseMetaData not implemented");
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {
    if (b) {
      throw new SQLFeatureNotSupportedException("Read-only mode is not supported");
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    // SQLite has no catalogs; ignore
  }

  @Override
  public String getCatalog() throws SQLException {
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // SQLite supports SERIALIZABLE (default) and can accept TRANSACTION isolation hints; not implemented
    throw new SQLFeatureNotSupportedException("setTransactionIsolation");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    // Default for SQLite
    return Connection.TRANSACTION_SERIALIZABLE;
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
  public Statement createStatement(int i, int i1) throws SQLException {
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
    return prepareStatement(s);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException("getTypeMap");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTypeMap");
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    if (i != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException("Only CLOSE_CURSORS_AT_COMMIT is supported");
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints not supported");
  }

  @Override
  public Statement createStatement(int i, int i1, int i2) throws SQLException {
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
    return prepareStatement(s);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException("CallableStatement is not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    return prepareStatement(s);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    return prepareStatement(s);
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    return prepareStatement(s);
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createClob");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createBlob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("createSQLXML");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return !closed;
  }

  @Override
  public void setClientInfo(String s, String s1) throws SQLClientInfoException {
    // ignore
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // ignore
  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return new Properties();
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLFeatureNotSupportedException("createArrayOf");
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLFeatureNotSupportedException("createStruct");
  }

  @Override
  public void setSchema(String s) throws SQLException {
    // ignore
  }

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int i) throws SQLException {
    // ignore
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
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
