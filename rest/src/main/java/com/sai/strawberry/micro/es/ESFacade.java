package com.sai.strawberry.micro.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sai.strawberry.micro.config.AppProperties;
import com.sai.strawberry.micro.model.ReactionRule;
import com.sai.strawberry.micro.model.ThenFetchQuery;
import com.sai.strawberry.micro.model.StreamingSearchConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sai
 */
public class ESFacade {
    private final AppProperties appProperties;
    private final static ObjectMapper JSONSERIALIZER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(ESFacade.class);

    @Inject
    public ESFacade(final AppProperties appProperties) throws Exception {
        this.appProperties = appProperties;
    }

    // Blocking API
    public Void init(final boolean forceRecreateEsIndex, final List<Map> configs) throws Exception {
        RestTemplate restTemplate = new RestTemplate();
        configs.forEach(eventConfig -> {
                    try {
                        StreamingSearchConfig config = JSONSERIALIZER.convertValue(eventConfig, StreamingSearchConfig.class);
                        if (forceRecreateEsIndex) {
                            try {
                                restTemplate.delete(appProperties.getEsUrl() + "/" + config.getDataCategoryName());
                            } catch (HttpClientErrorException ignored) {
                            }
                        }
                        if (isIndexMissing(restTemplate, config)) {
                            LOG.info("\n\n");

                            LOG.info("Creating es index: " + config.getDataCategoryName());
                            // create index.
                            restTemplate.postForObject(appProperties.getEsUrl() + "/" + config.getDataCategoryName(), "{}", Map.class, Collections.emptyMap());

                            LOG.info("Creating es mapping for the type: " + config.getDataCategoryName());
                            // apply mappings.
                            restTemplate.postForObject(appProperties.getEsUrl() + "/" + config.getDataCategoryName() + "/_mapping/" + config.getDataCategoryName(), JSONSERIALIZER.writeValueAsString(config.getEsIndexMappings()), Map.class, Collections.emptyMap());
                            LOG.info("\n\n");

                            List<ReactionRule> reactionRules = config.getReactionRules();
                            for (int i = 0; i < reactionRules.size(); i++) {
                                int id = i;
                                ReactionRule reactionRule = reactionRules.get(i);
                                Map percolateDoc = new LinkedHashMap();
                                Map whenMatchedQuery = reactionRule.getWhenMatchedQuery().getQuery();
                                List<ThenFetchQuery> reactQueries = reactionRule.getThenFetchQueries();
                                percolateDoc.put("query", whenMatchedQuery.get("query"));
                                percolateDoc.put("meta", reactQueries);
                                restTemplate.postForObject(appProperties.getEsUrl() + "/" + config.getDataCategoryName() + "/.percolator/" + id, JSONSERIALIZER.writeValueAsString(percolateDoc), Object.class, Collections.emptyMap());
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        throw new RuntimeException(ex);
                    }
                }
        );
        return null;
    }

    private boolean isIndexMissing(final RestTemplate restTemplate, final StreamingSearchConfig config) {
        try {
            restTemplate.headForHeaders(appProperties.getEsUrl() + "/" + config.getDataCategoryName());
        } catch (Exception ex) {
            return ex.getMessage().contains("404");
        }
        return false;
    }
}
