package org.suy.boltx.executer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import java.util.HashMap;

/**
 * Created by Masashi on 2017/06/13.
 */
public class BulkWriteCypherExecuter extends BulkCypherExecuter {

  public BulkWriteCypherExecuter(Driver driver, Vertx vertx){
    super(driver, vertx);
  }

  @Override
  public void execute(Message<JsonObject> msg){
    vertx.<JsonObject>executeBlocking(future -> {
      basicExecute(msg, future);
    }, false, res -> {

    });
  }

  protected void basicExecute(Message<JsonObject> msg,  Future<JsonObject> future){
    JsonObject bulkResult;
    try (Session session = driver.session())
    {
      try (Transaction tx = session.beginTransaction())
      {
        JsonObject body = msg.body();
        JsonArray statements = body.getJsonArray("statements");
        statements.forEach(each -> {
          this.processStatement((JsonObject) each, tx);
        });
        bulkResult = TrueValue;
        msg.reply(bulkResult);
      } catch (Neo4jException ex) {
        bulkResult = FalseValue;
        msg.reply(bulkResult);
      }
    }
    future.complete(bulkResult);
  }

  protected void processStatement(JsonObject statementJson, Transaction transaction) {
    basicProcessStatement((JsonObject)statementJson, transaction);
    transaction.success();
  }

}
