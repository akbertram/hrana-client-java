package com.bedatadriven.hrana;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static com.bedatadriven.hrana.proto.Hrana.*;

/**
 * Represents a single Hrana HTTP stream (a logical, stateful session) over the
 * stateless HTTP protocol.
 *
 * <p>This class implements the Hrana v3 HTTP pipeline as described in SPEC.md:
 * it serializes requests for a given stream and keeps the server-issued baton
 * between calls. The baton will be included with subsequent requests so the
 * server can associate them with the same stream. The server may also return a
 * new {@code base_url} to ensure stickiness; this instance updates its
 * {@code streamUrl} accordingly.</p>
 *
 * <p>Requests and responses are encoded using Protobuf and sent to the
 * {@code /v3-protobuf/pipeline} endpoint. See the generated protobuf types in
 * {@code com.bedatadriven.hrana.proto.Hrana}.</p>
 */
public class HranaHttpStream implements AutoCloseable {
  private String streamUrl;
  private final String jwt;
  private final HttpClient httpClient;

  private String baton;


  /**
   * Creates a new Hrana HTTP stream bound to a pipeline endpoint.
   *
   * @param streamUrl The pipeline URL, typically {@code <base>/v3-protobuf/pipeline}.
   *                  May be updated during the lifetime of the stream if the
   *                  server returns a different {@code base_url}.
   * @param jwt Optional bearer token that will be sent as
   *            {@code Authorization: Bearer <jwt>}.
   * @param httpClient The underlying {@link HttpClient} used to send requests.
   */
  HranaHttpStream(String streamUrl, String jwt, HttpClient httpClient) {
    this.streamUrl = streamUrl;
    this.jwt = jwt;
    this.httpClient = httpClient;
  }


  /**
   * Executes a single SQL statement with named parameters on this stream.
   *
   * <p>Parameter names should include the SQLite prefix character (":", "@" or "$"),
   * for example {@code :name}. The call is serialized on this stream and the
   * server-provided baton is sent automatically when available. Results produced
   * by the statement are currently ignored in this variant.</p>
   *
   * <p>Example:</p>
   * <pre>{@code
   * stream.executePrepared(
   *     "INSERT INTO users (name, age) VALUES (:name, :age)",
   *     Map.of(":name", "Alice", ":age", 30)
   * );
   * }</pre>
   *
   * @param sql The SQL statement text (single statement).
   * @param namedParams Map of named parameters to bind.
   * @throws IOException if the HTTP call fails or the server returns an HTTP error.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public long executePrepared(String sql, Map<String, Object> namedParams)
    throws IOException, InterruptedException {

    Stmt.Builder stmtBuilder = Stmt.newBuilder()
      .setSql(sql)
      .setWantRows(true);

    // Bind Named Arguments (:name, @name, $name)
    if (namedParams != null) {
      for (Map.Entry<String, Object> entry : namedParams.entrySet()) {
        Value value = convertToValue(entry.getValue());
        NamedArg arg = NamedArg.newBuilder()
          .setName(entry.getKey())
          .setValue(value)
          .build();
        stmtBuilder.addNamedArgs(arg);
      }
    }

    StmtResult result = executeStatement(stmtBuilder.build());

    return result.getAffectedRowCount();
  }


  /**
   * Executes a single SQL statement using the Hrana HTTP pipeline.
   *
   * <p>This helper is suitable for statements where result rows are not needed
   * (it sets {@code want_rows} to false). Use one of the {@code executePrepared}
   * overloads if you need to bind parameters or obtain a {@link StmtResult}.
   *
   * @param sql The SQL statement text to execute (single statement only).
   * @return number of affected rows.
   * @throws IOException          if the HTTP call fails or the server returns an HTTP error.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public long executeStatement(String sql) throws IOException, InterruptedException {
    // Construct the Stmt (SQL text)
    Stmt stmt = Stmt.newBuilder()
      .setSql(sql)
      .setWantRows(false)
      .build();

    StmtResult streamResult = executeStatement(stmt);

    return streamResult.getAffectedRowCount();
  }

  /**
   * Executes a list of SQL statements as a batch on this stream, without
   * conditions and without returning any rows.
   *
   * <p>Each SQL string must contain a single statement. The server will execute
   * them sequentially. If any step fails, this method throws an IOException.
   * No result rows are requested or returned.</p>
   *
   * @param sqlStatements List of SQL statements to execute.
   * @throws IOException if the HTTP call fails or the server reports an error for any step.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public void executeStatements(String... sqlStatements) throws IOException, InterruptedException {
    if (sqlStatements.length == 0) {
      return; // nothing to do
    }

    // Build the Batch with steps that have no condition and want_rows=false
    Batch.Builder batchBuilder = Batch.newBuilder();
    for (String sql : sqlStatements) {
      Stmt stmt = Stmt.newBuilder()
        .setSql(sql)
        .setWantRows(false)
        .build();
      BatchStep step = BatchStep.newBuilder()
        .setStmt(stmt) // no condition set => unconditional
        .build();
      batchBuilder.addSteps(step);
    }

    BatchStreamReq batchReq = BatchStreamReq.newBuilder()
      .setBatch(batchBuilder.build())
      .build();

    StreamRequest streamReq = StreamRequest.newBuilder()
      .setBatch(batchReq)
      .build();

    List<StreamResult> streamResults = request(streamReq);

    for (StreamResult result : streamResults) {
      if (result.hasError()) {
        throw new IOException("Hrana error: " + result.getError().getMessage());
      }
      if (result.hasOk()) {
        StreamResponse ok = result.getOk();
        if (ok.hasBatch()) {
          BatchResult br = ok.getBatch().getResult();
          // If any step error is present, throw the first one encountered
          if (!br.getStepErrorsMap().isEmpty()) {
            // Pick the lowest step index for determinism
            int firstStep = br.getStepErrorsMap().keySet().stream().min(Integer::compareTo).orElse(0);
            com.bedatadriven.hrana.proto.Hrana.Error err = br.getStepErrorsMap().get(firstStep);
            String msg = (err != null && !err.getMessage().isEmpty()) ? err.getMessage() : ("Batch step " + firstStep + " failed");
            throw new IOException("Hrana batch error: " + msg);
          }
        }
      }
    }
    // Otherwise: success, nothing to return
  }


  /**
   * Executes a prepared statement with positional parameters.
   * Example: executePrepared("INSERT INTO users (name, age) VALUES (?, ?)",
   * List.of("Bob", 25));
   *
   * @param sql The SQL statement text (single statement).
   * @param positionalParams Positional parameter values in order of appearance.
   * @return The {@link StmtResult} returned by the server, including columns,
   *         rows (if any), and counters like affected row count.
   * @throws IOException if the HTTP call fails or the server returns an HTTP error.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public StmtResult executePrepared(String sql, List<Object> positionalParams)
    throws IOException, InterruptedException {

    Stmt.Builder stmtBuilder = Stmt.newBuilder()
      .setSql(sql)
      .setWantRows(true);

    // Bind Positional Arguments (?)
    if (positionalParams != null) {
      for (Object param : positionalParams) {
        stmtBuilder.addArgs(convertToValue(param));
      }
    }

    return executeStatement(stmtBuilder.build());
  }

  /**
   * Converts a Java value into the corresponding Hrana {@link Value} protobuf.
   * Supported types: null, String, Integer/Long, Float/Double, byte[], Boolean.
   * Booleans are mapped to integers 0/1 as SQLite has no native boolean type.
   *
   * @throws IllegalArgumentException for unsupported types.
   */
  private Value convertToValue(Object obj) {
    Value.Builder val = Value.newBuilder();

    if (obj == null) {
      val.setNull(Value.Null.getDefaultInstance());
    } else if (obj instanceof String) {
      val.setText((String) obj);
    } else if (obj instanceof Integer || obj instanceof Long) {
      val.setInteger(((Number) obj).longValue());
    } else if (obj instanceof Float || obj instanceof Double) {
      val.setFloat(((Number) obj).doubleValue());
    } else if (obj instanceof byte[]) {
      val.setBlob(ByteString.copyFrom((byte[]) obj));
    } else if (obj instanceof Boolean) {
      // SQLite has no native BOOLEAN type; typically handled as integer 0 or 1
      val.setInteger((Boolean) obj ? 1 : 0);
    } else {
      throw new IllegalArgumentException("Unsupported type: " + obj.getClass());
    }
    return val.build();
  }

  /**
   * Sends an {@link ExecuteStreamReq} request wrapped in a pipeline, including
   * the current baton if present, and returns the parsed {@link StmtResult}.
   */
  private StmtResult executeStatement(Stmt stmt) throws IOException, InterruptedException {
    // Wrap in StreamRequest -> PipelineReqBody
    StreamRequest streamReq = StreamRequest.newBuilder()
      .setExecute(ExecuteStreamReq.newBuilder().setStmt(stmt))
      .build();

    StreamResult streamResult = request(streamReq).get(0);
    if (streamResult.hasError()) {
      throw new IOException("Hrana error: " + streamResult.getError().getMessage());
    } else if (streamResult.hasOk()) {
      return streamResult.getOk().getExecute().getResult();
    } else {
      throw new IOException("Unexpected response from server: " + streamResult);
    }
  }


  /**
   * Closes this stream on the server by sending a {@code close} request.
   *
   * <p>If the stream has not yet been used and no baton was issued, this method
   * is a no-op. When a baton is present, it is included to identify which
   * server-side stream to close.</p>
   *
   * @throws IOException if the HTTP call fails or the server returns an HTTP error.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public void close() throws IOException, InterruptedException {

    // If we haven't been issued a baton, there is nothing to close
    if (this.baton == null) {
      return;
    }

    CloseStreamReq closeStreamReq = CloseStreamReq.newBuilder().build();

    // Wrap in the generic StreamRequest union
    StreamRequest streamReq = StreamRequest.newBuilder()
      .setClose(closeStreamReq)
      .build();

    StreamResult streamResult = request(streamReq).get(0);
    if (streamResult.hasError()) {
      throw new IOException("Hrana error: " + streamResult.getError().getMessage());
    }
  }

  /**
   * Sends a pipeline request to the current {@code streamUrl} with required
   * headers (content type, accept, authorization) and returns the raw response.
   */
  private List<StreamResult> request(StreamRequest... requests) throws IOException, InterruptedException {

    PipelineReqBody.Builder body = PipelineReqBody.newBuilder();
    for (StreamRequest request : requests) {
      body.addRequests(request);
    }

    if(this.baton != null) {
      body.setBaton(this.baton);
    }

    HttpRequest httpRequest = HttpRequest.newBuilder()
      .uri(URI.create(this.streamUrl))
      .POST(HttpRequest.BodyPublishers.ofByteArray(body.build().toByteArray()))
      .header("Authorization", "Bearer " + this.jwt)
      .header("Content-Type", "application/x-protobuf")
      .header("Accept", "application/x-protobuf")
      .build();

    HttpResponse<byte[]> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());

    if (httpResponse.statusCode() < 200 || httpResponse.statusCode() >= 300) {
      throw new IOException("HTTP Error: " + httpResponse.statusCode());
    }

    PipelineRespBody pipelineResponse = PipelineRespBody.parseFrom(httpResponse.body());

    if (pipelineResponse.hasBaton()) {
      this.baton = pipelineResponse.getBaton();
    }
    if (pipelineResponse.hasBaseUrl()) {
      this.streamUrl = pipelineResponse.getBaseUrl() + "/v3-protobuf/pipeline";
    }
    return pipelineResponse.getResultsList();
  }


  /**
   * Queries the server for the current autocommit state of this stream.
   *
   * <p>Returns {@code true} if the stream is currently in the autocommit state
   * (i.e., not inside an explicit transaction) and {@code false} otherwise.</p>
   *
   * @return whether the server reports autocommit mode for this stream.
   * @throws IOException if the HTTP call fails or the server returns an error response.
   * @throws InterruptedException if the thread is interrupted while waiting for the response.
   */
  public boolean getAutocommit() throws IOException, InterruptedException {
    // Build the GetAutocommit request
    GetAutocommitStreamReq getReq = GetAutocommitStreamReq.newBuilder().build();

    StreamRequest streamReq = StreamRequest.newBuilder()
      .setGetAutocommit(getReq)
      .build();

    StreamResult result = request(streamReq).get(0);

    if (result.hasError()) {
      // surface the server error as IOException to caller
      throw new IOException("Hrana error: " + result.getError().getMessage());
    } else if (result.hasOk()) {
      StreamResponse ok = result.getOk();
      if (ok.hasGetAutocommit()) {
        return ok.getGetAutocommit().getIsAutocommit();
      }
    }
    throw new IOException("No get_autocommit response returned by server");
  }
}
