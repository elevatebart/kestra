package io.kestra.core.runners;

import jakarta.validation.constraints.NotNull;

import java.util.Objects;
import java.util.UUID;

/**
 * Runtime information about a Kestra server.
 */
public record ServerInstance(@NotNull UUID id) {
    private static final ServerInstance INSTANCE = new ServerInstance(UUID.randomUUID());

    public ServerInstance {
        Objects.requireNonNull(id, "id cannot be null");
    }

    /**
     * @return the local {@link ServerInstance}.
     */
    public static ServerInstance getInstance() {
        return INSTANCE;
    }

}
