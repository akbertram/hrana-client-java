# hrana-client-java

![Maven Central Version](https://img.shields.io/maven-central/v/com.bedatadriven/hrana-client)

A lightweight Java client that speaks the Hrana protocol over HTTP to LibSQL/Turso-compatible 
backends. You can use it in two ways:

- Low-level: call the Hrana HTTP pipeline directly via `HranaHttpClient` and `HranaHttpStream`.
- JDBC: use the minimal JDBC driver to work with `java.sql` (`Connection`, `Statement`, `PreparedStatement`, `ResultSet`).

This project is still under development and will probably require some additional work before the
JDBC interface will work completely.

## Installation

Add the following maven dependency:

```.xml
<dependency>
    <groupId>com.bedatadriven</groupId>
    <artifactId>hrana-client</artifactId>
    <version>0.1</version>
</dependency>
```


## Features
- JDBC `Driver` for `jdbc:libsql://...` URLs
- Low-level API: `HranaHttpClient`/`HranaHttpStream` for direct Hrana v3 over HTTP (no JDBC)
- HTTP/2 client using Java 11+ `HttpClient`
- Basic SQL execution via `Statement` and `PreparedStatement`
- Transaction support with `setAutoCommit`, `commit`, `rollback`
- JWT-based authentication
- Tested against Hrana v3 protobuf pipeline

## Choosing between low-level API and JDBC
- Choose low-level (`HranaHttpClient`/`HranaHttpStream`) when you want:
  - Minimal surface area and control over Hrana-specific features (pipeline, baton, stickiness)
  - Smaller dependency footprint and straightforward use in serverless or non-blocking contexts
  - Direct access to protobuf results (`StmtResult`) and manual mapping
- Choose JDBC when you want:
  - Compatibility with existing code that uses `java.sql.*`
  - Familiar `Connection`/`Statement`/`PreparedStatement`/`ResultSet` APIs
  - Easier migration path for legacy applications

## Requirements
- Java 17 or newer
- A LibSQL/Turso-compatible endpoint that supports Hrana over HTTPS
- A JWT token with appropriate permissions

## Quick start (JDBC)
The driver registers automatically when the JAR is on the classpath, so you can connect
with `DriverManager`:

```java
import java.sql.*;
import java.util.Properties;

public class Demo {
  public static void main(String[] args) throws Exception {
    // Option A: JWT in the URL
    try (Connection conn = DriverManager.getConnection(
        "jdbc:libsql://mydb.turso.io/my_namespace?jwt=YOUR_TOKEN")) {
      runDemo(conn);
    }

    // Option B: JWT via Properties
    Properties props = new Properties();
    props.setProperty("jwt", System.getenv("HRANA_JWT"));
    try (Connection conn = DriverManager.getConnection(
        "jdbc:libsql://mydb.turso.io/my_namespace", props)) {
      runDemo(conn);
    }

    // Option C: JWT via system property or env var HRANA_JWT
    //   -DHRANA_JWT=YOUR_TOKEN  or  export HRANA_JWT=YOUR_TOKEN
    try (Connection conn = DriverManager.getConnection(
        "jdbc:libsql://mydb.turso.io/my_namespace")) {
      runDemo(conn);
    }
  }

  private static void runDemo(Connection conn) throws Exception {
    try (Statement st = conn.createStatement()) {
      st.executeUpdate("CREATE TABLE IF NOT EXISTS demo (id INTEGER PRIMARY KEY, name TEXT)");
      st.executeUpdate("DELETE FROM demo");
      st.executeUpdate("INSERT INTO demo (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
      try (ResultSet rs = st.executeQuery("SELECT id, name FROM demo ORDER BY id")) {
        while (rs.next()) {
          System.out.println(rs.getLong(1) + ": " + rs.getString(2));
        }
      }
    }
  }
}
```

## Using the low-level HranaHttpClient directly
Some applications don't need JDBC APIs and prefer a small, explicit client that talks Hrana v3 over HTTP. For that, use `HranaHttpClient` and `HranaHttpStream`.

- When to choose this:
  - You want minimal overhead and direct access to Hrana concepts (pipeline, baton, base_url stickiness).
  - You are building your own repository/DAO layer or a serverless function where JDBC is inconvenient.
- When to prefer JDBC: if you have existing code that already uses `java.sql.*` or you need the familiar `Connection/PreparedStatement/ResultSet` APIs.

### Quick start (low-level)
```java
import com.bedatadriven.hrana.HranaHttpClient;
import com.bedatadriven.hrana.HranaHttpStream;
import com.bedatadriven.hrana.proto.Hrana.StmtResult;
import java.util.List;
import java.util.Map;

public class LowLevelDemo {
  public static void main(String[] args) throws Exception {
    String baseUrl = "https://mydb.turso.io/my_namespace"; // or normalize your libsql:// to https://
    String jwt = System.getenv("HRANA_JWT");

    HranaHttpClient client = new HranaHttpClient(baseUrl, jwt);
    try (HranaHttpStream stream = client.newStream()) {
      // Simple statements without result rows
      stream.executeStatement("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
      stream.executeStatement("DELETE FROM users");

      // Positional parameters
      stream.executePrepared(
          "INSERT INTO users (name, age) VALUES (?, ?)",
          List.of("Alice", 30)
      );

      // Named parameters (remember to include the prefix :/@/$ in the key)
      stream.executePrepared(
          "INSERT INTO users (name, age) VALUES (:name, :age)",
          Map.of(":name", "Bob", ":age", 25)
      );

      // Query with rows
      StmtResult result = stream.executePrepared(
          "SELECT id, name, age FROM users WHERE age >= ? ORDER BY id",
          List.of(20)
      );
      // Inspect columns and rows (protobuf model)
      System.out.println("cols: " + result.getColsList().size());
      System.out.println("rows: " + result.getRowsList().size());

      // Autocommit state (true means not inside an explicit transaction)
      boolean isAutocommit = stream.getAutocommit();
      System.out.println("autocommit: " + isAutocommit);

      // Transactions can be controlled with SQL
      stream.executeStatement("BEGIN");
      stream.executePrepared("INSERT INTO users (name, age) VALUES (?, ?)", List.of("Carol", 22));
      stream.executeStatement("COMMIT");
    }
  }
}
```

Notes for the low-level API:
- Streams are stateful over HTTP using a server-issued baton; the stream includes it automatically on subsequent requests.
- The server can return a new `base_url` for stickiness; the stream updates its internal URL accordingly.
- Requests are serialized per stream; do not issue concurrent calls on the same `HranaHttpStream`.
- Each call must contain exactly one SQL statement; multi-statement strings (with semicolons) are not supported by the protocol.
- For types, booleans are encoded as 0/1 integers; blobs use `byte[]`.
- Errors can be HTTP-level (non-2xx) or Hrana-level (returned in the protobuf body). Methods throw `IOException` for both kinds.
- `executeStatement(String)` sets `want_rows=false` and ignores rows; use `executePrepared(sql, List)` to obtain a `StmtResult` with rows.

## JDBC URL format and authentication
Supported URL format:

```
jdbc:libsql://host[:port][/namespace][?jwt=TOKEN]
```

JWT token resolution priority (first non-empty wins):
1. URL query parameter `jwt`
2. `Properties` passed to `DriverManager.getConnection(url, props)` under key `"jwt"`
3. System property `HRANA_JWT`
4. Environment variable `HRANA_JWT`

Notes:
- The driver internally converts `libsql://...` to the HTTPS base URL expected by the HTTP client.
- Any trailing slash is trimmed automatically.

## Examples
- Statement:

```java
try (Statement st = conn.createStatement()) {
  st.executeUpdate("INSERT INTO users (id, name) VALUES (1, 'Alice')");
  try (ResultSet rs = st.executeQuery("SELECT id, name FROM users ORDER BY id")) {
    while (rs.next()) {
      long id = rs.getLong(1);
      String name = rs.getString(2);
    }
  }
}
```

- PreparedStatement with parameters and binary data:

```java
try (PreparedStatement ps = conn.prepareStatement(
    "INSERT INTO files (id, name, data) VALUES (?, ?, ?)") ) {
  ps.setInt(1, 10);
  ps.setString(2, "readme.txt");
  ps.setBytes(3, new byte[]{1,2,3});
  ps.executeUpdate();
}
```

- Transactions:

```java
conn.setAutoCommit(false);
try (PreparedStatement ps = conn.prepareStatement(
    "INSERT INTO logs (id, msg) VALUES (?, ?)") ) {
  ps.setLong(1, 200);
  ps.setString(2, "Persisted");
  ps.executeUpdate();
}
conn.commit();
conn.setAutoCommit(true);
```

## Running integration tests
Integration tests require environment variables:

- `HRANA_URL`: Base HTTPS URL (e.g. `https://YOUR-DB.turso.io/your_ns` or `libsql://...`).
- `HRANA_JWT`: JWT token with permissions to create/modify tables.

Run:

```bash
HRANA_URL="https://YOUR-DB.turso.io/your_ns" \
HRANA_JWT="YOUR_TOKEN" \
./gradlew test --info
```

The tests will normalize a `libsql://` value to `https://` automatically.

## Limitations and status
- Not a full JDBC-compliant driver (`Driver#jdbcCompliant()` returns false)
- No `CallableStatement`
- Limited type mapping; focused on common SQLite/LibSQL types
- Only Hrana over HTTP v3 protobuf pipeline is supported

## Troubleshooting
- "No suitable driver" — ensure this JAR is on the classpath; the service file `META-INF/services/java.sql.Driver` must be present. You can also force load with:
  ```java
  Class.forName("com.bedatadriven.hrana.HranaDriver");
  ```
- Auth errors — verify your JWT and that it has privileges for the target namespace.
- Endpoint URL — ensure you use your database host and namespace; trailing slashes are fine but will be trimmed.


