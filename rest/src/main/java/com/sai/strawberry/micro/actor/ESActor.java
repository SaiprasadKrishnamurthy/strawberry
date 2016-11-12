package com.sai.strawberry.micro.actor;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.micro.config.ActorFactory;
import com.sai.strawberry.micro.es.ESFacade;
import com.sai.strawberry.micro.model.StreamingSearchConfig;
import scala.concurrent.Future;

import java.util.List;
import java.util.Map;

/**
 * Created by saipkri on 08/09/16.
 */
public class ESActor extends UntypedActor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static long timeout_in_seconds = 5 * 1000;
    private final ActorFactory actorFactory;
    private final ESFacade esFacade;

    public ESActor(final ActorFactory actorFactory, final ESFacade esFacade) {
        this.actorFactory = actorFactory;
        this.esFacade = esFacade;
    }

    @Override
    public void onReceive(final Object forceRecreateEsIndex) throws Throwable {
        if (forceRecreateEsIndex instanceof Boolean) {
            ActorRef repositoryActor = actorFactory.newActor(RepositoryActor.class);
            Future<Object> allConfigsFuture = Patterns.ask(repositoryActor, StreamingSearchConfig.class, RepositoryActor.timeout_in_seconds);
            allConfigsFuture.onComplete(new OnComplete<Object>() {
                @Override
                public void onComplete(Throwable failure, Object success) throws Throwable {
                    List<Map> configs = (List<Map>) success;
                    esFacade.init((Boolean) forceRecreateEsIndex, configs);
                }
            }, actorFactory.executionContext());
            getSender().tell(true, getSelf());
        }
    }
}
