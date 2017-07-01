package org.suy.boltx.starter;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.eventbus.bridge.tcp.TcpEventBusBridge;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.suy.boltx.executer.CypherExecuter;
import org.suy.boltx.executer.SingleResponseCypherExecuter;

public class MainVerticle extends AbstractVerticle {

  static final String Boltx_port_key = "boltx.port";
  static final String Boltx_bolt_uri_key = "boltx.bolt.uri";
  static final String Boltx_neo4j_username_key = "boltx.neo4j.username";
  static final String Boltx_neo4j_password_key = "boltx.neo4j.password";


  protected Driver driver;
  private final static  Logger log = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start() {
    log.info("starting");
    ConfigRetriever retriever = this.createConfigRetriever();
    retriever.getConfig(ar -> {
      if (ar.failed()) {
        log.error("Failed to get config");
      } else {
        startWithConfig(ar.result());
      }
    });
  }

  @Override
  public void stop() {
    log.info("stopping");
    driver.close();
  }

  protected void startWithConfig(JsonObject config) {
    log.info("config: " + config);
    if (setupBoltDriver(config)) return;
    this.setupConsumers();
    setupEventBusBridge(config);
  }

  protected boolean setupBoltDriver(JsonObject config) {
    String uri = config.getString(Boltx_bolt_uri_key);
    String username = config.getString(Boltx_neo4j_username_key);
    String password = config.getString(Boltx_neo4j_password_key);

    try {
      driver = GraphDatabase.driver( uri, AuthTokens.basic( username, password ) );
    } catch (Neo4jException ex) {
      log.error("Neo4jException");
      log.error(ex);
      return true;
    }
    return false;
  }

  protected void setupEventBusBridge(JsonObject config) {
    TcpEventBusBridge bridge = this.createTcpEventBusBridge();
    Integer port = config.getInteger(Boltx_port_key);
    bridge.listen(port, res -> log.info("Ready: "+ bridge));
  }

  protected ConfigRetriever createConfigRetriever() {
    ConfigRetrieverOptions options = new ConfigRetrieverOptions();
    options
      .addStore(
        new ConfigStoreOptions().setType("json")
          .setConfig(createDefaultConfig()))
      .addStore(
        new ConfigStoreOptions().setType("json")
          .setConfig(vertx.getOrCreateContext().config()))
      .addStore(
        new ConfigStoreOptions().setType("sys")
      )
      .addStore(new ConfigStoreOptions().setType("env")
      );
    return ConfigRetriever.create(vertx, options);
  }

  protected JsonObject createDefaultConfig() {
    //vertx.fileSystem().readFile(path, completionHandler);
    return new JsonObject()
      .put(Boltx_bolt_uri_key, "bolt://localhost:7687")
      .put(Boltx_neo4j_username_key, "neo4j")
      .put(Boltx_neo4j_password_key, "neo4j")
      .put(Boltx_port_key, 7000)
      ;
  }

  protected TcpEventBusBridge createTcpEventBusBridge() {
    return TcpEventBusBridge.create(
      vertx,
      new BridgeOptions()
        .addInboundPermitted(new PermittedOptions().setAddress("executeAndReturnCypher"))
        .addOutboundPermitted(new PermittedOptions().setAddress("executeAndReturnCypher"))
        .addInboundPermitted(new PermittedOptions().setAddress("executeCypher"))
        .addOutboundPermitted(new PermittedOptions().setAddress("returnCypher")));
  }

  protected void setupConsumers() {
    vertx.eventBus().consumer("executeAndReturnCypher", (Message<JsonObject> msg) -> {
      SingleResponseCypherExecuter executer = new SingleResponseCypherExecuter(driver, vertx);
      executer.execute(msg);
    });

    vertx.eventBus().consumer("executeCypher", (Message<JsonObject> msg) -> {
      CypherExecuter executer = new CypherExecuter(driver, vertx);
      executer.execute(msg);
    });
  }
}
