package com.sai.strawberry.micro.rest;

import akka.actor.ActorRef;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.micro.config.ActorFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import com.sai.strawberry.micro.actor.KafkaProducerActor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import scala.concurrent.Future;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by saipkri on 08/07/16.
 */
@Api("Rest API to submit the data as a stream to be searched on the move")
@RestController
public class StreamingSearchResource {

    private final ActorFactory actorFactory;

    private static final ObjectMapper m = new ObjectMapper();

    @Inject
    public StreamingSearchResource(final ActorFactory actorFactory) {
        this.actorFactory = actorFactory;
    }

    @ApiOperation("Submits the data asynchronously to be searched real-time using streaming search.")
    @CrossOrigin(methods = {RequestMethod.POST, RequestMethod.PUT, RequestMethod.OPTIONS, RequestMethod.GET})
    @RequestMapping(value = "/streamingsearch/{dataCategoryName}/{dataIdentifier}", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
    public DeferredResult<ResponseEntity<?>> submitData(@RequestBody final Object payload, @PathVariable("dataCategoryName") final String dataCategoryName, @PathVariable("dataIdentifier") final String dataIdentifier, @RequestParam("groupedFieldName") final String groupedFieldName) throws Exception {
        DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<>(5000L);
        ActorRef kafkaProducerActor = actorFactory.newActor(KafkaProducerActor.class);
        Map<String, Object> request = new HashMap<>();
        request.put("topic", dataCategoryName);
        request.put("payload", payload);
        request.put("dataIdentifier", dataIdentifier);
        request.put("partitionKey", groupedFieldName);

        // Find the right config.

        Future<Object> submitPayloadToKafkaTopicFuture = Patterns.ask(kafkaProducerActor, request, KafkaProducerActor.timeout_in_seconds);

        submitPayloadToKafkaTopicFuture.onComplete(new OnComplete<Object>() {

            @Override
            public void onComplete(Throwable failure, Object success) throws Throwable {
                if (failure != null || !(Boolean) success) {
                    deferredResult.setResult(new ResponseEntity<>("Message: " + failure != null ? failure.getMessage() : "NA", HttpStatus.BAD_REQUEST));
                } else {
                    deferredResult.setResult(new ResponseEntity<>(HttpStatus.OK));
                }
            }
        }, actorFactory.executionContext());
        return deferredResult;
    }
}
