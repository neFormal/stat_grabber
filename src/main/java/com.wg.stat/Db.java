package com.wg.stat;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Consumer;

public class Db extends AbstractBehavior<Db.DbCommand> {

    public static interface DbCommand {}
    public static class Get implements DbCommand {
        public ActorRef from;
        public Get(ActorRef ref) { from = ref; }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Receive createReceive() {
        return receiveBuilder()
                .onMessage(Get.class, message -> {
                    context.getLog().info("on get: {}", message);
                    try {
                        Connection conn = dataSource.getConnection();
                        message.from.tell(conn);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    return Behaviors.same();
                })
                .onMessage(DbCommand.class, message -> {
                    context.getLog().info("unknown message: {}", message);
                    return Behaviors.same();
                })
                .onSignal(PostStop.class, message -> {
                    dataSource.close();
                    return Behaviors.same();
                })
                .build();
    }

    private final ActorContext context;
    private HikariDataSource dataSource;

    static final ServiceKey<Db> serviceKey = ServiceKey.create(Db.class, "dbService");

    private Db(ActorContext context) {
        this.context = context;

        context.getLog().info("create Db Service");

        context.getSystem().receptionist().tell(Receptionist.register(serviceKey, context.getSelf()));

        HikariConfig config = new HikariConfig("hikari.properties");
        dataSource = new HikariDataSource(config);
    }

    public static Behavior behavior() {
        return Behaviors.setup(Db::new);
    }

    public static Behavior<Receptionist.Listing> recept(Consumer<ActorRef> callback) {
        return Behaviors.setup(context -> {
            context.getLog().debug("create recept actor");

            return Behaviors.receive(Object.class)
                    .onMessage(Receptionist.Listing.class,
                            listing -> {
                                context.getLog().debug("on listing predicate");
                                return listing.isForKey(serviceKey);
                            },
                            (ctx, listing) -> {
                                context.getLog().debug("on message listing");
                                listing.getServiceInstances(serviceKey).forEach(s -> callback.accept(s));
                                return Behaviors.stopped();
                            })
                    .build();
        }).narrow();
    }

    @FunctionalInterface
    public interface SQLConsumer<T> {
        void accept(T t) throws SQLException;
    }

    public static void getConnect(ActorContext context, SQLConsumer<Connection> callback) {

        ActorRef ref = context.spawnAnonymous(recept(dbActorRef -> {

            AskPattern.ask(
                    dbActorRef,
                    Get::new,
                    Duration.ofSeconds(5),
                    context.getSystem().scheduler()
            )
            .whenComplete((c, fail) -> {
                Connection conn = (Connection)c;
                context.getLog().debug("recept conn: {}", conn);
                context.getLog().debug("recept fail: {}", fail);

                try {
                    callback.accept(conn);
                } catch (SQLException e) {
                    context.getLog().error(e, "exception on callback");

                    try {
                        conn.close();
                    } catch (SQLException e2) {
                        context.getLog().error(e2, "exception on close connection");
                    }
                }
            });
        }));
        context.getSystem().receptionist().tell(Receptionist.find(serviceKey, ref));
    }
}
