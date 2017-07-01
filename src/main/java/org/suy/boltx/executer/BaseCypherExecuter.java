package org.suy.boltx.executer;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 * Created by Masashi on 2017/06/23.
 */
public abstract class BaseCypherExecuter {
  protected Driver driver;
  protected Vertx vertx;

  public BaseCypherExecuter(Driver driver, Vertx vertx) {
    this.driver = driver;
    this.vertx = vertx;
  }

  public abstract void execute(Message<JsonObject> msg);

  protected StatementResult processRequest(Message<JsonObject> msg, Session session) {
    JsonObject body = msg.body();
    JsonObject params = body.getJsonObject("parameters");
    StatementResult sr;
    if(params==null){
      sr = session.run(body.getString("statement"));
    } else {
      sr = session.run(body.getString("statement"), params.getMap());
    }
    return sr;
  }
}
