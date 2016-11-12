package com.sai.strawberry.micro.config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import com.sai.strawberry.micro.actor.ESActor;
import org.apache.kafka.clients.producer.KafkaProducer;
import com.sai.strawberry.micro.actor.KafkaProducerActor;
import com.sai.strawberry.micro.actor.RepositoryActor;
import com.sai.strawberry.micro.es.ESFacade;
import scala.concurrent.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by saipkri on 08/07/16.
 */
public class ActorFactory {

    private Map<String, ActorRef> actors = new HashMap<>();

    private final ActorSystem actorSystem;
    private final AppProperties appProperties;
    private final ESFacade esFacade;


    public ActorFactory(final ActorSystem actorSystem, final AppProperties appProperties, final KafkaProducer<String, String> kafkaTemplate, final ESFacade esFacade) {
        this.actorSystem = actorSystem;
        this.appProperties = appProperties;
        this.esFacade = esFacade;
        // Create the actor pool.
        actors.put(KafkaProducerActor.class.getName(), actorSystem.actorOf(Props.create(KafkaProducerActor.class, kafkaTemplate, this).withRouter(new RoundRobinPool(appProperties.getConcurrencyFactor()))));
        actors.put(RepositoryActor.class.getName(), actorSystem.actorOf(Props.create(RepositoryActor.class).withRouter(new RoundRobinPool(appProperties.getConcurrencyFactor()))));
        actors.put(ESActor.class.getName(), actorSystem.actorOf(Props.create(ESActor.class, this, esFacade).withRouter(new RoundRobinPool(appProperties.getConcurrencyFactor()))));
    }

    public <T> ActorRef newActor(final Class<T> actorType) {
        return actors.get(actorType.getName());
    }

    public ExecutionContext executionContext() {
        return actorSystem.dispatcher();
    }
}
