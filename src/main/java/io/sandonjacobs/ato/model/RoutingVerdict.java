package io.sandonjacobs.ato.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** The JSON we ask the LLM to answer with. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoutingVerdict {
    public String team;
    public String priority;
    public String reasoning;

    public RoutingVerdict() {}
}
