package com.sai.strawberry.micro.model;

import lombok.Data;

import java.util.List;

/**
 * Created by saipkri on 24/10/16.
 */
@Data
public class ReactionRule {
    private WhenMatchedQuery whenMatchedQuery;
    private List<ThenFetchQuery> thenFetchQueries;
}
