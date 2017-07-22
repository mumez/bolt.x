package org.suy.boltx.executer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.Neo4jException;

/**
 * Created by Masashi on 2017/06/13.
 */
public class BulkCypherExecuter extends CypherExecuter {

  protected String prevStatement = "";

  public BulkCypherExecuter(Driver driver, Vertx vertx){
    super(driver, vertx);
  }

  @Override
  public void execute(Message<JsonObject> msg){
    vertx.<JsonObject>executeBlocking(future -> {
      basicExecute(msg, future);
    }, false, res -> {
      this.returnBulkResultFrom(msg.body(), res.result());
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
      } catch (Neo4jException ex) {
        bulkResult = FalseValue;
      }
    }
    future.complete(bulkResult);
  }

  protected void processStatement(JsonObject statementJson, Transaction transaction) {
    StatementResult result = basicProcessStatement((JsonObject)statementJson, transaction);
    transaction.success();
    returnResultFrom((JsonObject)statementJson, result);
  }

  protected void returnBulkResultFrom(JsonObject messageBody, JsonObject data){
    JsonObject replyObj = new JsonObject();
    replyObj.put("requestId", messageBody.getString("requestId"));
    replyObj.put("data", data);
    replyObj.put("end", true);
    vertx.eventBus().publish("returnCypher", replyObj);
  }


  protected StatementResult basicProcessStatement(JsonObject statementJson, Transaction transaction) {
    JsonObject params = statementJson.getJsonObject("parameters");
    String statement = statementJson.getString("statement", prevStatement);

    StatementResult sr;
    if(params==null){
      sr = transaction.run(statement);
    } else {
      sr = transaction.run(statement, params.getMap());
    }
    prevStatement = statement;
    return sr;
  }

}
