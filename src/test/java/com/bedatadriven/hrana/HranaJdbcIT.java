package com.bedatadriven.hrana;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for HranaConnection that exercise basic JDBC flows over the Hrana HTTP protocol.
 *
 * Environment variables:
 * - HRANA_URL: Base URL of the Hrana HTTP endpoint (e.g. https://YOUR-DB.turso.io or https://...bunnydb.net)
 *              If you have a libsql:// URL, it will be normalized to https:// automatically.
 * - HRANA_JWT: JWT token with permissions to create/modify tables in the target namespace.
 */
public class HranaJdbcIT {

  private String baseUrl;
  private String jwt;

  private Connection connection;
  private String tableName;

  @BeforeEach
  void setUp() throws Exception {
    baseUrl = System.getenv("HRANA_URL");
    jwt = System.getenv("HRANA_JWT");

    assertTrue(baseUrl != null && !baseUrl.isBlank(),
      "HRANA_URL not set; integration test cannot run");
    assertTrue(jwt != null && !jwt.isBlank(),
      "HRANA_JWT not set; integration test cannot run");

    this.connection = DriverManager.getConnection("jdbc:" + baseUrl);

    tableName = "it_users_" + UUID.randomUUID().toString().replace('-', '_');
    try (Statement st = this.connection.createStatement()) {
      st.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (id INTEGER PRIMARY KEY, name TEXT, note BLOB)");
      // Ensure empty table at start of each test
      st.executeUpdate("DELETE FROM " + tableName);
    }
  }

  private static String normalizeUrl(String url) {
    String u = url.trim();
    if (u.startsWith("libsql://")) {
      u = "https://" + u.substring("libsql://".length());
    }
    // remove trailing / to avoid double slashes; HranaHttpClient will append the path it needs
    return u.replaceAll("/$", "");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null) {
      try (Statement st = connection.createStatement()) {
        // Drop the table created for this test to leave DB clean
        st.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      } catch (SQLException ignored) {
        // ignore cleanup errors
      }
      connection.close();
    }
  }

  @Test
  void statement_insert_and_select() throws Exception {
    try (Statement st = connection.createStatement()) {
      int ins1 = st.executeUpdate("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice')");
      int ins2 = st.executeUpdate("INSERT INTO " + tableName + " (id, name) VALUES (2, 'Bob')");
      assertTrue(ins1 >= 0);
      assertTrue(ins2 >= 0);

      try (ResultSet rs = st.executeQuery("SELECT id, name FROM " + tableName + " ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals("Alice", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals("Bob", rs.getString(2));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void preparedStatement_parameters_and_types() throws Exception {
    try (PreparedStatement ps = connection.prepareStatement(
      "INSERT INTO " + tableName + " (id, name, note) VALUES (?, ?, ?)")) {
      ps.setInt(1, 10);
      ps.setString(2, "Charlie");
      ps.setBytes(3, new byte[] {1,2,3});
      int count = ps.executeUpdate();
      assertTrue(count >= 0);
    }

    try (PreparedStatement ps = connection.prepareStatement(
      "SELECT id, name, note FROM " + tableName + " WHERE id = ?")) {
      ps.setLong(1, 10);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(10L, rs.getLong(1));
        assertEquals("Charlie", rs.getString(2));
        byte[] note = (byte[]) rs.getObject(3);
        assertArrayEquals(new byte[] {1,2,3}, note);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void transaction_rollback_discards_changes() throws Exception {
    connection.setAutoCommit(false);
    try (PreparedStatement ps = connection.prepareStatement(
      "INSERT INTO " + tableName + " (id, name) VALUES (?, ?)")) {
      ps.setInt(1, 100);
      ps.setString(2, "Temp");
      ps.executeUpdate();
    }
    connection.rollback();

    try (PreparedStatement ps = connection.prepareStatement(
      "SELECT COUNT(*) FROM " + tableName + " WHERE id = 100")) {
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0L, rs.getLong(1));
      }
    }
    // switch back just in case
    connection.setAutoCommit(true);
  }

  @Test
  void transaction_commit_persists_changes() throws Exception {
    connection.setAutoCommit(false);
    try (PreparedStatement ps = connection.prepareStatement(
      "INSERT INTO " + tableName + " (id, name) VALUES (?, ?)")) {
      ps.setInt(1, 200);
      ps.setString(2, "Persisted");
      ps.executeUpdate();
    }
    connection.commit();

    try (PreparedStatement ps = connection.prepareStatement(
      "SELECT name FROM " + tableName + " WHERE id = 200")) {
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Persisted", rs.getString(1));
      }
    }
    connection.setAutoCommit(true);
  }

  @Disabled
  @Test
  void connection_lifecycle_and_defaults() throws Exception {
    assertTrue(connection.getAutoCommit());
    assertEquals("SELECT 1", connection.nativeSQL("SELECT 1"));
    assertTrue(connection.isValid(1));

    connection.close();
    assertTrue(connection.isClosed());
  }
}
