package org.suy.boltx.executer;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.suy.boltx.converter.RecordToJsonObjectConverter;

/**
 * Created by Masashi on 2017/06/13.
 */
public class CypherExecuter extends BaseCypherExecuter {

  protected RecordToJsonObjectConverter converter;

  public CypherExecuter(Driver driver, Vertx vertx){
    super(driver, vertx);
    this.converter = new RecordToJsonObjectConverter();
  }

  public void execute(Message<JsonObject> msg){
    try ( Session session = driver.session() )
    {
      StatementResult sr = processRequest(msg, session);
      this.returnResultFrom(msg.body(), sr);
    }
  }

  protected void returnResultFrom(JsonObject messageBody, StatementResult sr){
    EventBus eb = this.vertx.eventBus();
    Integer counter = 0;
    while(sr.hasNext()){
      Record record = sr.next();
      JsonObject replyObj = new JsonObject();
      replyObj.put("requestId", messageBody.getString("requestId"));
      replyObj.put("data", this.converter.convert(record));
      replyObj.put("counter", counter++);
      if(!sr.hasNext()){
        replyObj.put("end", true);
      }
      eb.publish("returnCypher", replyObj);
    }
  }


}
