package org.suy.boltx.starter;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameHelper;
import io.vertx.ext.eventbus.bridge.tcp.impl.protocol.FrameParser;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

  private Vertx vertx;

  @Before
  public void setUp(TestContext tc) {
    vertx = Vertx.vertx();
    vertx.deployVerticle(MainVerticle.class.getName(), tc.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  @Test
  public void testExecuteAndReturnCypher(TestContext context) {
    // Send a request and get a response
    NetClient client = vertx.createNetClient();
    final Async async = context.async();

    client.connect(7000, "localhost", conn -> {
      context.assertFalse(conn.failed());

      NetSocket socket = conn.result();

      final FrameParser parser = new FrameParser(parse -> {
        context.assertTrue(parse.succeeded());
        JsonObject frame = parse.result();
        context.assertNotEquals("err", frame.getString("type"));
        JsonObject ret = frame.getJsonObject("body");
        context.assertEquals("{\"data\":[{\"ret\":[1,2,3]}]}", ret.toString());
        client.close();
        async.complete();
      });

      socket.handler(parser::handle);

      JsonObject payload = new JsonObject().put("statement", "WITH [1,2,3] as ret RETURN ret");

      FrameHelper.sendFrame("send", "executeAndReturnCypher", UUID.randomUUID().toString(), payload, socket);
    });
  }

}
