package org.suy.boltx.executer;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Transaction;

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
    }, true, res -> {
      msg.reply(res.result());
    });
  }

  protected void processStatement(JsonObject statementJson, Transaction transaction) {
    basicProcessStatement((JsonObject)statementJson, transaction);
    transaction.success();
  }

}
