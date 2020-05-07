package pw.edu.pl.mgr.pawel.zak.reactive.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pw.edu.pl.mgr.pawel.zak.reactive.constans.ErrorCodesEnum;
import pw.edu.pl.mgr.pawel.zak.reactive.constans.PairEnum;
import pw.edu.pl.mgr.pawel.zak.reactive.constans.SqlQueryEnum;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;

public class DatabaseVerticle extends AbstractVerticle {

  public static final String CONFIG_SQL_QUERIES_RESOURCE_FILE = "sqlqueries.resource.file";

  public static final String CONFIG_DB_QUEUE = "db.queue";
  public static final String CONFIG_DB_PORT = "db.port";
  public static final String CONFIG_DB_HOST = "db.host";
  public static final String CONFIG_DB_NAME = "db.name";
  public static final String CONFIG_DB_USER = "db.user";
  public static final String CONFIG_DB_PASSWORD = "db.password";

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseVerticle.class);

  private PgPool client;

  private final HashMap<SqlQueryEnum, String> sqlQueries = new HashMap<>();

  @Override
  public void start(Promise<Void> promise) throws Exception {

    loadSqlQueries();

    PgConnectOptions connectOptions = new PgConnectOptions()
      .setPort(config().getInteger(CONFIG_DB_PORT, 5432))
      .setHost(config().getString(CONFIG_DB_HOST, "host.docker.internal"))
//      .setHost("localhost")
      .setDatabase(config().getString(CONFIG_DB_NAME, "postgres"))
      .setUser(config().getString(CONFIG_DB_USER, "postgres"))
      .setPassword(config().getString(CONFIG_DB_PASSWORD, "password"));

    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(5);

    client = PgPool.pool(vertx, connectOptions, poolOptions);

    client.getConnection(ar1 -> {

      if (ar1.succeeded()) {

        LOGGER.info("DB Connected");
        client.query("select count(*) from rate", ar -> {
          if (ar.succeeded()) {
            RowSet<Row> rows = ar.result();
            LOGGER.info("Database rate values number: " + rows.iterator().next().getInteger(0));
          } else {
            LOGGER.error("Could not open a database connection", ar.cause());
          }
        });
        vertx.eventBus().consumer(config().getString(CONFIG_DB_QUEUE, "db.queue"), this::onMessage);  // <3>
        promise.complete();

      } else {
        LOGGER.error("Could not open a database connection", ar1.cause());
        promise.fail(ar1.cause());
      }
    });
  }

  private void loadSqlQueries() throws IOException {

    String queriesFile = config().getString(CONFIG_SQL_QUERIES_RESOURCE_FILE);
    InputStream queriesInputStream;
    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(queriesInputStream);
    queriesInputStream.close();

    sqlQueries.put(SqlQueryEnum.GET_RATE, queriesProps.getProperty("get-rate"));
    sqlQueries.put(SqlQueryEnum.GET_AVERAGE_HIGH, queriesProps.getProperty("get-average-high"));
    sqlQueries.put(SqlQueryEnum.GET_AVERAGE_LOW, queriesProps.getProperty("get-average-low"));
    sqlQueries.put(SqlQueryEnum.GET_AVERAGE_OPEN, queriesProps.getProperty("get-average-open"));
    sqlQueries.put(SqlQueryEnum.GET_AVERAGE_CLOSE, queriesProps.getProperty("get-average-close"));
  }

  private void onMessage(Message<JsonObject> message) {

    if (!message.headers().contains("action")) {
      LOGGER.error("No action header specified for message with headers {} and body {}",
        message.headers(), message.body().encodePrettily());
      message.fail(ErrorCodesEnum.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
      return;
    }
    String action = message.headers().get("action");

    switch (action) {
      case "get-rate":
        fetchRate(message);
        break;
      case "get-average":
        fetchAverage(message);
        break;
      default:
        message.fail(ErrorCodesEnum.BAD_ACTION.ordinal(), "Bad action: " + action);
    }
  }

  private void fetchRate(Message<JsonObject> message) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    LocalDateTime dateTime = LocalDateTime.parse(message.body().getString("date_time"), formatter);

    String pair = message.body().getString("pair");

    Tuple params = Tuple.of(PairEnum.valueOf(pair).getIndx(), dateTime);

    client.preparedQuery(sqlQueries.get(SqlQueryEnum.GET_RATE), params, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        RowSet rows = fetch.result();
        if (rows.rowCount() == 0) {
          response.put("found", false);
        } else {
          response.put("found", true);
          Row row = (Row) rows.iterator().next();
          response.put("high", row.getDouble(0));
          response.put("low", row.getDouble(1));
          response.put("t_open", row.getDouble(2));
          response.put("t_close", row.getDouble(3));
        }
        message.reply(response);
      } else {
        reportQueryError(message, fetch.cause());
      }
    });
  }


  private void fetchAverage(Message<JsonObject> message) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    LocalDateTime from = LocalDateTime.parse(message.body().getString("from"), formatter);
    LocalDateTime until = LocalDateTime.parse(message.body().getString("until"), formatter);
    String pair = message.body().getString("pair");
    String dimension = message.body().getString("dimension");

    SqlQueryEnum querry = null;

    switch (dimension) {
      case "high":
        querry = SqlQueryEnum.GET_AVERAGE_HIGH;
        break;
      case "low":
        querry = SqlQueryEnum.GET_AVERAGE_LOW;
        break;
      case "open":
        querry = SqlQueryEnum.GET_AVERAGE_OPEN;
        break;
      case "close":
        querry = SqlQueryEnum.GET_AVERAGE_CLOSE;
        break;
      default:
        message.fail(ErrorCodesEnum.BAD_ACTION.ordinal(), "Bad dimension: " + dimension);
    }

    Tuple params = Tuple.of(
      PairEnum.valueOf(pair).getIndx()
      , from
      , until);

    client.preparedQuery(sqlQueries.get(querry), params, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();

        RowSet rows = fetch.result();
        Row row = (Row) rows.iterator().next();
        Double value = row.getDouble(0);

        if (value == null) {
          response.put("found", false);
        } else {
          response.put("found", true);
          response.put("value", row.getDouble(0));
        }
        message.reply(response);
      } else {
        reportQueryError(message, fetch.cause());
      }
    });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database query error", cause);
    message.fail(ErrorCodesEnum.DB_ERROR.ordinal(), cause.getMessage());
  }
}
