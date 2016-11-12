package com.sai.strawberry.micro.rest;

import akka.actor.ActorRef;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.micro.actor.ESActor;
import com.sai.strawberry.micro.actor.RepositoryActor;
import com.sai.strawberry.micro.config.ActorFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import com.sai.strawberry.micro.es.ESFacade;
import com.sai.strawberry.micro.model.StreamingSearchConfig;
import com.sai.strawberry.micro.util.CallbackFunctionLibrary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import scala.concurrent.Future;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by saipkri on 08/07/16.
 */
@Api("Rest API for the audit config microservice")
@RestController
public class ConfigResource {

    private final ActorFactory actorFactory;
    private final ESFacade esInitializer;

    private static final ObjectMapper m = new ObjectMapper();

    @Inject
    public ConfigResource(final ActorFactory actorFactory, final ESFacade esInitializer) {
        this.actorFactory = actorFactory;
        this.esInitializer = esInitializer;
    }

    @ApiOperation("Gets all the audit processing configs configured in the system")
    @CrossOrigin(methods = {RequestMethod.POST, RequestMethod.PUT, RequestMethod.OPTIONS, RequestMethod.GET})
    @RequestMapping(value = "/configs", method = RequestMethod.GET, produces = "application/json")
    public DeferredResult<ResponseEntity<List<StreamingSearchConfig>>> allEventConfigs() throws Exception {
        DeferredResult<ResponseEntity<List<StreamingSearchConfig>>> deferredResult = new DeferredResult<>(5000L);
        ActorRef repositoryActor = actorFactory.newActor(RepositoryActor.class);

        Future<Object> results = Patterns.ask(repositoryActor, StreamingSearchConfig.class, RepositoryActor.timeout_in_seconds);
        OnFailure failureCallback = CallbackFunctionLibrary.onFailure(t -> deferredResult.setErrorResult(new ResponseEntity<>(t.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR)));

        results.onSuccess(new OnSuccess<Object>() {
            public void onSuccess(final Object results) {
                deferredResult.setResult(new ResponseEntity<>((List<StreamingSearchConfig>) results, HttpStatus.OK));
            }
        }, actorFactory.executionContext());

        results.onFailure(failureCallback, actorFactory.executionContext());
        return deferredResult;
    }

    @ApiOperation("Resyncs the configs stored in the system with elasticsearch")
    @CrossOrigin(methods = {RequestMethod.POST, RequestMethod.PUT, RequestMethod.OPTIONS, RequestMethod.GET})
    @RequestMapping(value = "/configs/resync", method = RequestMethod.PUT, produces = "application/json")
    public DeferredResult<ResponseEntity<?>> resyncConfigs(@RequestParam("forceRecreateEsIndex") final boolean forceRecreateEsIndex) throws Exception {
        DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<>(10000L);
        ActorRef esActor = actorFactory.newActor(ESActor.class);
        Future<Object> results = Patterns.ask(esActor, forceRecreateEsIndex, ESActor.timeout_in_seconds);
        OnFailure failureCallback = CallbackFunctionLibrary.onFailure(t -> deferredResult.setErrorResult(new ResponseEntity<>(t.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR)));

        results.onSuccess(new OnSuccess<Object>() {
            public void onSuccess(final Object results) {
                deferredResult.setResult(new ResponseEntity<>(HttpStatus.CREATED));
            }
        }, actorFactory.executionContext());

        results.onFailure(failureCallback, actorFactory.executionContext());
        return deferredResult;
    }
}
