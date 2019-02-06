package com.wg.stat;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.Behaviors;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {

        Behavior<Void> main = Behaviors.setup(context -> {
            context.getLog().info("main setup");

            ActorRef dbService = context.spawn(Db.behavior(), "Db");
            context.watch(dbService);

            ActorRef<GatherService.GatherServiceCommand> gatherService = context.spawn(GatherService.behavior(), "GatherService");
            context.watch(gatherService);

            gatherService.tell(new GatherService.Test("new message"));
            gatherService.tell(new GatherService.AddWorker());
//            gatherService.tell(new GatherService.AddWorker());
//            gatherService.tell(new GatherService.AddWorker());

            return Behaviors.<Void>receiveSignal((ctx, sig) -> {
                ctx.getLog().info("signal received: {}", sig);
                if (sig instanceof Terminated)
                    return Behaviors.stopped();
                else
                    return Behaviors.unhandled();
            });
        });

        final ActorSystem system = ActorSystem.create(main, "stat");
        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
            system.log().info(system.printTree());
        } catch (IOException ioe) {
        } finally {
            system.terminate();
        }
    }
}
