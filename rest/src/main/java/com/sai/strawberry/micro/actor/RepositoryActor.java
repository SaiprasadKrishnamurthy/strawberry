package com.sai.strawberry.micro.actor;

import akka.actor.UntypedActor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.micro.model.StreamingSearchConfig;

import java.util.List;

/**
 * Created by saipkri on 08/09/16.
 */
public class RepositoryActor extends UntypedActor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static long timeout_in_seconds = 5 * 1000;


    @Override
    public void onReceive(final Object message) throws Throwable {
        if (message instanceof Class && ((Class) message).getName().equals(StreamingSearchConfig.class.getName())) {
            getSender().tell(MAPPER.readValue(RepositoryActor.class.getClassLoader().getResourceAsStream("StreamingSearchConfigs.json"), List.class), getSelf());
        }
    }
}
