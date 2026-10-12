package io.sandonjacobs.ato.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

/** The teams a ticket can be routed to, loaded from {@code teams.json} on the classpath. */
public final class TeamDirectory {

    public static final String FALLBACK_TEAM = "triage";

    public record Team(String id, String name, String owns) {}

    private static final List<Team> TEAMS = load();

    private TeamDirectory() {}

    public static List<Team> teams() {
        return TEAMS;
    }

    public static boolean isKnown(String teamId) {
        return TEAMS.stream().anyMatch(t -> t.id().equals(teamId));
    }

    /** Renders the directory as a bullet list for the system prompt. */
    public static String asPromptText() {
        return TEAMS.stream()
                .map(t -> String.format("- %s (%s): %s", t.id(), t.name(), t.owns()))
                .collect(Collectors.joining("\n"));
    }

    private static List<Team> load() {
        try (InputStream in = TeamDirectory.class.getClassLoader().getResourceAsStream("teams.json")) {
            if (in == null) {
                throw new IllegalStateException("teams.json not found on classpath");
            }
            return new ObjectMapper().readValue(in, new TypeReference<List<Team>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
