package com.wg.stat;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.io.StringReader;

public class GatherWorker extends AbstractBehavior {

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .onMessage(String.class, message -> {
                    context.getLog().info("worker got: {}", message);
//                    if (message.equals("die"))
//                        throw new Exception(message);

                    Db.getConnect(context, conn -> {
                        StringReader reader = null;

                        try {
                            String data = NginxAccessLog.parseToCsv("access.log", "datetime", "ip", "msg", "a1", "a2");
                            context.getLog().info("data: {}", data);
                            reader = new StringReader(data);

                            PgConnection pgConn = conn.unwrap(PgConnection.class);
                            pgConn.getCopyAPI().copyIn("copy log1 (datetime, ip, msg, a1, a2) from stdin with csv", reader);
                        } catch (IOException e) {
                            context.getLog().error(e, "worker failed on processing");
                        }
                    });
                    return Behaviors.same();
                })
                .build();
    }

    private final ActorContext context;
    private GatherWorker(ActorContext context) {
        this.context = context;

        context.getLog().info("create worker");
    }

    static Behavior behavior() {
        return Behaviors.setup(GatherWorker::new);
    }
}
