package pw.edu.pl.mgr.pawel.zak.reactive.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class HttpServerVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  private static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  private static final String CONFIG_DB_QUEUE = "db.queue";

  private String dbQueue = "db.queue";

  @Override
  public void start(Promise<Void> promise) throws Exception {

    dbQueue = config().getString(CONFIG_DB_QUEUE, "db.queue");

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/random").handler(this::randomHandler);
    router.get("/rate/:pair/:date_time").handler(this::rateHandler);
    router.get("/mediumRate/:pair/:dimension/:from/:until").handler(this::averageRateHandler);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server
      .requestHandler(router)
      .listen(portNumber, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port " + portNumber);
          promise.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          promise.fail(ar.cause());
        }
      });
  }

  private void randomHandler(RoutingContext context) {
    context.response().setStatusCode(200);
    JsonObject response = new JsonObject()
      .put("value", ThreadLocalRandom.current().nextDouble());

    context.response().end(response.encode());
  }

  private void rateHandler(RoutingContext context) {

    String dateTime = context.request().getParam("date_time");
    String pair = context.request().getParam("pair");
    JsonObject request = new JsonObject().put("date_time", dateTime).put("pair", pair);

    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-rate");

    vertx.eventBus().request(dbQueue, request, options, reply -> {
      JsonObject response = new JsonObject();

      if (reply.succeeded()) {
        JsonObject rate = (JsonObject) reply.result().body();

        if (rate.getBoolean("found")) {
          response
            .put("high", rate.getDouble("high"))
            .put("low", rate.getDouble("low"))
            .put("t_open", rate.getDouble("t_open"))
            .put("t_close", rate.getDouble("t_close"))
            .put("pair", pair)
            .put("date_time", dateTime);
          context.response().setStatusCode(200);
        } else {
          context.response().setStatusCode(404);
          response
            .put("success", false)
            .put("error", "There is no data for day " + dateTime + "and pair " + pair);
        }
      } else {
        response
          .put("success", false)
          .put("error", reply.cause().getMessage());
        context.response().setStatusCode(500);
      }
      context.response().putHeader("Content-Type", "application/json");
      context.response().end(response.encode());
    });
  }

  private void averageRateHandler(RoutingContext context) {

    String pair = context.request().getParam("pair");
    String dimension = context.request().getParam("dimension");
    String requestedFrom = context.request().getParam("from");
    String requestedUntil = context.request().getParam("until");

    JsonObject request = new JsonObject()
      .put("pair", pair)
      .put("dimension", dimension)
      .put("from", requestedFrom)
      .put("until", requestedUntil);

    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-average");
    vertx.eventBus().request(dbQueue, request, options, reply -> {
      JsonObject response = new JsonObject();

      if (reply.succeeded()) {
        JsonObject rate = (JsonObject) reply.result().body();

        if (rate.getBoolean("found")) {
          response
            .put("value", rate.getDouble("value"))
            .put("pair", pair)
            .put("from", requestedFrom)
            .put("until", requestedUntil)
            .put("dimension", dimension);
          context.response().setStatusCode(200);
        } else {
          context.response().setStatusCode(404);
          response
            .put("success", false)
            .put("error", "There is no data for days " + requestedFrom + " " + requestedUntil + " for pair " + pair);
        }
      } else {
        response
          .put("success", false)
          .put("error", reply.cause().getMessage());
        context.response().setStatusCode(500);
      }
      context.response().putHeader("Content-Type", "application/json");
      context.response().end(response.encode());
    });
  }
}
