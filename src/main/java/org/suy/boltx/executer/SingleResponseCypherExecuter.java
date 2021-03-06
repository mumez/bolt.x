package org.suy.boltx.executer;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.suy.boltx.converter.StatementResultToJsonObjectConverter;

/**
 * Created by Masashi on 2017/06/13.
 */
public class SingleResponseCypherExecuter extends BaseCypherExecuter {

  protected StatementResultToJsonObjectConverter converter;

  public SingleResponseCypherExecuter(Driver driver, Vertx vertx){
    super(driver, vertx);
    this.converter = new StatementResultToJsonObjectConverter();
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
    try ( Session session = driver.session() ) {
      StatementResult sr = processRequest(msg, session);
      future.complete(converter.convert(sr));
    }
  }

}
