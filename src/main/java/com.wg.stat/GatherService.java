package com.wg.stat;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class GatherService extends AbstractBehavior {

    private Map<ActorRef, ActorRef> workers = new HashMap<>(10);

    private Behavior timer;

    private final ActorContext<GatherServiceCommand> context;
    private GatherService(ActorContext<GatherServiceCommand> context) {
        this.context = context;

        timer = Behaviors.withTimers(timers -> idle(timers, this));
        context.spawn(timer, "timer").tell(5);
    }

    private static final Object TIMER_KEY = new Object();
    private static Behavior idle(TimerScheduler timers, GatherService service) {
        return Behaviors.receive(Integer.class)
            .onMessage(Integer.class, (ctx, delay) -> {
                ctx.getLog().info("timer tick: {}", delay);

                timers.startSingleTimer(TIMER_KEY, delay, Duration.ofSeconds(delay));

                for (ActorRef ref : service.workers.keySet())
                {
                    ref.tell("die");
                }

                return idle(timers, service);
            })
            .build();
    }

    static interface GatherServiceCommand {}
    public static final class AddWorker implements GatherServiceCommand { }
    public static final class Test implements GatherServiceCommand {
        public String msg;
        public Test(String msg) {
            this.msg = msg;
        }
    }

    public static final Behavior<GatherServiceCommand> behavior() {
        return Behaviors.setup(GatherService::new);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Receive createReceive() {
        return receiveBuilder()
                .onMessage(Test.class, message -> {
                    context.getLog().info("got: {}", message);
                    return Behaviors.same();
                })
                .onMessage(AddWorker.class, message -> {
                    Behavior workerBehavior = Behaviors.supervise(GatherWorker.behavior())
                            .onFailure(SupervisorStrategy.restartWithLimit(1, Duration.ofSeconds(1)));
//                    Behavior workerBehavior = GatherWorker.behavior();
                    ActorRef<GatherWorker> worker = context.spawnAnonymous(workerBehavior);
                    workers.put(worker, worker);

                    context.getLog().info("workers count: {}", workers.size());

                    context.watch(worker);
                    return Behaviors.same();
                })
                .onMessage(Exception.class, e -> {
                    context.getLog().info("exception: {}", e);
                    return Behaviors.same();
                })
                .onSignal(Terminated.class, message -> {
                    ActorRef worker = ((ChildFailed)message).getRef();
                    context.getLog().info("worker terminated: {}", worker);
                    workers.remove(worker);
                    context.getLog().info("workers count: {}", workers.size());
                    return Behaviors.same();
                })
                .onSignalEquals(PreRestart.instance(), () -> {
                    context.getLog().info("gather service pre-restart");
                    return Behaviors.same();
                })
                .onMessage(Object.class, message -> {
                    context.getLog().info("unknown message: {}", message);
                    return Behaviors.same();
                })
                .build();
    }
}
