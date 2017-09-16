package me.kiril.mesos.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class State<FwId, TaskId, TaskState> {
//    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    private final double cpusPerTask;
    private final double memMbPerTask;
    @NotNull
    private final String resourceRole;

    @NotNull
    private final Map<TaskId, TaskState> taskStates;
    @NotNull
    private final AtomicInteger offerCounter = new AtomicInteger();
    @NotNull
    private final AtomicInteger totalTaskCounter = new AtomicInteger();

    @NotNull
    private final FwId fwId;

    public State(
        @NotNull final FwId fwId,
        @NotNull final String resourceRole,
        final double cpusPerTask,
        final double memMbPerTask
    ) {
        this.fwId = fwId;
        this.resourceRole = resourceRole;
        this.cpusPerTask = cpusPerTask;
        this.memMbPerTask = memMbPerTask;
        this.taskStates = new ConcurrentHashMap<>();
    }

    @NotNull
    public FwId getFwId() {
        return fwId;
    }

    public double getCpusPerTask() {
        return cpusPerTask;
    }

    public double getMemMbPerTask() {
        return memMbPerTask;
    }

    @NotNull
    public String getResourceRole() {
        return resourceRole;
    }

    @NotNull
    public AtomicInteger getOfferCounter() {
        return offerCounter;
    }

    @NotNull
    public AtomicInteger getTotalTaskCounter() {
        return totalTaskCounter;
    }

    public void put(final TaskId key, final TaskState value) {
        taskStates.put(key, value);
    }
}