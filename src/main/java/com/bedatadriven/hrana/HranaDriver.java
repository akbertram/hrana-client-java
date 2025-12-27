package com.bedatadriven.hrana;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for Hrana/LibSQL over HTTP.
 *
 * Supports URLs of the form:
 *   jdbc:libsql://host[:port][/namespace][?jwt=TOKEN]
 *
 * JWT can be provided via (priority order):
 *   1) URL query parameter "jwt"
 *   2) Properties passed to DriverManager with key "jwt"
 *   3) System property "HRANA_JWT"
 *   4) Environment variable "HRANA_JWT"
 */
public class HranaDriver implements Driver {

  public static final String URL_PREFIX = "jdbc:libsql://";

  static {
    try {
      DriverManager.registerDriver(new HranaDriver());
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null; // per Driver contract
    }

    Parsed parsed = parseUrl(url, info);

    HranaHttpClient client = new HranaHttpClient(parsed.baseHttpsUrl, parsed.jwt);
    HranaHttpStream stream = client.newStream();
    return new HranaConnection(stream);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url != null && url.toLowerCase(Locale.ROOT).startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    List<DriverPropertyInfo> list = new ArrayList<>();
    DriverPropertyInfo jwtProp = new DriverPropertyInfo("jwt", valueOrNull(info, "jwt"));
    jwtProp.description = "JWT auth token for the LibSQL/Turso database";
    jwtProp.required = false;
    list.add(jwtProp);
    return list.toArray(new DriverPropertyInfo[0]);
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false; // we don't implement full JDBC compliance
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // Optional for non-compliant drivers
    throw new SQLFeatureNotSupportedException("Logging not integrated");
  }

  private static String valueOrNull(Properties props, String key) {
    return props == null ? null : props.getProperty(key);
  }

  private static class Parsed {
    final String baseHttpsUrl;
    final String jwt;
    Parsed(String baseHttpsUrl, String jwt) {
      this.baseHttpsUrl = baseHttpsUrl;
      this.jwt = jwt;
    }
  }

  private static Parsed parseUrl(String url, Properties info) throws SQLException {
    // Strip the jdbc: prefix; retain the libsql:// URL including potential query
    String libsql = url.substring("jdbc:".length()); // assumes acceptsURL already
    URI uri;
    try {
      uri = new URI(libsql);
    } catch (URISyntaxException e) {
      throw new SQLException("Invalid JDBC URL syntax: " + url, e);
    }

    // Convert libsql://... -> https://...
    String https = "https://" + uri.getRawSchemeSpecificPart().substring(2); // skip "//"

    // Remove query and trailing slash to form base URL; HranaHttpClient appends its own path
    String withoutQuery = https;
    int q = withoutQuery.indexOf('?');
    if (q >= 0) {
      withoutQuery = withoutQuery.substring(0, q);
    }
    // remove trailing slash
    if (withoutQuery.endsWith("/")) {
      withoutQuery = withoutQuery.substring(0, withoutQuery.length() - 1);
    }

    // Determine JWT: query param jwt, then props, then system property, then env
    String jwtFromQuery = null;
    String rawQuery = uri.getRawQuery();
    if (rawQuery != null && !rawQuery.isEmpty()) {
      for (String part : rawQuery.split("&")) {
        int eq = part.indexOf('=');
        String k = eq >= 0 ? part.substring(0, eq) : part;
        String v = eq >= 0 ? part.substring(eq + 1) : "";
        if (k.equals("jwt")) {
          jwtFromQuery = decodePercents(v);
        }
      }
    }

    String jwt = firstNonEmpty(jwtFromQuery,
      valueOrNull(info, "jwt"),
      System.getProperty("HRANA_JWT"),
      System.getenv("HRANA_JWT"));

    return new Parsed(withoutQuery, jwt);
  }

  private static String firstNonEmpty(String... values) {
    if (values == null) return null;
    for (String v : values) {
      if (v != null && !v.isEmpty()) return v;
    }
    return null;
  }

  private static String decodePercents(String s) {
    try {
      return java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return s; // fallback
    }
  }
}
