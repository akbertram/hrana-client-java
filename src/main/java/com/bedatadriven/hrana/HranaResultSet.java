package com.bedatadriven.hrana;

import com.bedatadriven.hrana.proto.Hrana;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

class HranaResultSet implements ResultSet {
  private final Hrana.StmtResult result;

  private int currentRowIndex = -1;
  private Hrana.Row currentRow;
  private Map<String, Integer> columnLabelMap = null;
  private boolean wasNull;
  private boolean closed = false;

  public HranaResultSet(Hrana.StmtResult result) {
    this.result = result;
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if(columnLabelMap == null) {
      columnLabelMap = new HashMap<>();
      for (int i = 0; i < result.getColsList().size(); i++) {
        columnLabelMap.put(result.getCols(i).getName(), i + 1);
      }
    }
    Integer index = columnLabelMap.get(columnLabel);
    if(index == null) {
      throw new SQLException("No such column '" + columnLabel + "'.");
    }

    return index;
  }

  @Override
  public boolean next() throws SQLException {
    currentRowIndex++;
    if (currentRowIndex < result.getRowsCount()) {
      currentRow = result.getRows(currentRowIndex);
      return true;
    } else {
      currentRow = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
    // NOOP
    closed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public String getString(int i) throws SQLException {
    Hrana.Value value = currentRow.getValues(i - 1);
    String text = null;
    if(value.hasText()) {
      text = value.getText();
    }
    wasNull = (text == null);
    return text;
  }

  @Override
  public long getLong(int i) {
    Hrana.Value value = currentRow.getValues(i - 1);
    if(value.hasInteger()) {
      wasNull = false;
      return value.getInteger();
    } else if(value.hasFloat()) {
      wasNull = false;
      return (long) value.getFloat();
    } else {
      wasNull = true;
      return 0L;
    }
  }

  @Override
  public Object getObject(int i) throws SQLException {
    Hrana.Value value = currentRow.getValues(i - 1);
    if(value.hasNull()) {
      wasNull = true;
      return null;
    }
    wasNull = false;
    if(value.hasInteger()) {
      return value.getInteger();
    } else if(value.hasFloat()) {
      return value.getFloat();
    } else if(value.hasBlob()) {
      return value.getBlob().toByteArray();
    } else {
      throw new IllegalStateException("value = " + value);
    }
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(int i) throws SQLException {
    return getLong(i) != 0L;
  }

  @Override
  public int getInt(int i) throws SQLException {
    long value = getLong(i);
    if(value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
      throw new SQLException("Integer (" + value + ") out of range for int");
    }
    return (int)value;
  }

  @Override
  public byte getByte(int i) throws SQLException {
    long value = getLong(i);
    if(value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
      throw new SQLException("Integer (" + value + ") out of range for byte");
    }
    return (byte)value;
  }

  @Override
  public short getShort(int i) throws SQLException {
    long value = getLong(i);
    if(value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
      throw new SQLException("Integer (" + value + ") out of range for short");
    }
    return (short)value;
  }

  @Override
  public double getDouble(int i) throws SQLException {
    Hrana.Value value = currentRow.getValues(i - 1);
    if(value.hasInteger()) {
      wasNull = false;
      return value.getInteger();
    } else if(value.hasFloat()) {
      wasNull = false;
      return value.getFloat();
    } else {
      wasNull = true;
      return 0L;
    }
  }

  @Override
  public float getFloat(int i) throws SQLException {
    return (float) getDouble(i);
  }

  @Override
  public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public byte[] getBytes(int i) throws SQLException {
    Hrana.Value value = currentRow.getValues(i - 1);
    if(value.hasBlob()) {
      wasNull = false;
      return value.getBlob().toByteArray();
    } else if(value.hasNull()) {
      wasNull = true;
      return new byte[0];
    } else {
      throw new UnsupportedOperationException("type: " + value);
    }
  }

  @Override
  public Date getDate(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Time getTime(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Timestamp getTimestamp(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return currentRowIndex < 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return currentRowIndex >= result.getRowsCount();
  }

  @Override
  public boolean isFirst() throws SQLException {
    return currentRowIndex == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    return currentRowIndex + 1 == result.getRowsCount();
  }

  @Override
  public void beforeFirst() throws SQLException {
    currentRowIndex = -1;
    currentRow = null;
  }

  @Override
  public void afterLast() throws SQLException {
    currentRowIndex = result.getRowsCount();
  }

  @Override
  public boolean first() throws SQLException {
    currentRowIndex = -1;
    return next();
  }

  @Override
  public boolean last() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int getRow() throws SQLException {
    if (currentRowIndex < 0) {
      return 0;
    } else {
      return currentRowIndex + 1;
    }
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean relative(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void setFetchSize(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int getType() throws SQLException {
    return TYPE_SCROLL_INSENSITIVE;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int i, byte b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int i, short i1) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void updateInt(int i, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int i, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int i, float v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int i, double v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int i, String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int i, Date date) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int i, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o, int i1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal bigDecimal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String columnLabel1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date date) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp timestamp) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream inputStream, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream inputStream, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object o, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object o) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Array getArray(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public URL getURL(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref ref) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob blob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int i, Array array) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array array) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    return HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public void updateNString(int i, String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String columnLabel1) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML sqlxml) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int i) throws SQLException {
    return getString(i);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getString(columnLabel);
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(int i, Class<T> aClass) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> aClass) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new UnsupportedOperationException("TODO");
  }
}
