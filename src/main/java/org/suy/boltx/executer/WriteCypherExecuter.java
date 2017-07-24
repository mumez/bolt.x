package org.suy.boltx.executer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.Neo4jException;

/**
 * Created by Masashi on 2017/06/13.
 */
public class WriteCypherExecuter extends BaseCypherExecuter {

  public WriteCypherExecuter(Driver driver, Vertx vertx){
    super(driver, vertx);
  }

  @Override
  public void execute(Message<JsonObject> msg){
    vertx.<JsonObject>executeBlocking(future -> {
      basicExecute(msg, future);
    }, false, res -> {
      msg.reply(res.result());
    });
  }

  protected void basicExecute(Message<JsonObject> msg, Future<JsonObject> future) {
    JsonObject bulkResult;
    try (Session session = driver.session())
    {
      try (Transaction tx = session.beginTransaction())
      {
        StatementResult sr = processRequest(msg, tx);
        tx.success();
        bulkResult = TrueValue;
      } catch (Neo4jException ex) {
        bulkResult = FalseValue;
        log.error(ex);
      }
    }
    future.complete(bulkResult);
  }

  protected StatementResult processRequest(Message<JsonObject> msg, Transaction transaction) {
    JsonObject body = msg.body();
    JsonObject params = body.getJsonObject("parameters");
    StatementResult sr;
    if(params==null){
      sr = transaction.run(body.getString("statement"));
    } else {
      sr = transaction.run(body.getString("statement"), params.getMap());
    }
    return sr;
  }

}
