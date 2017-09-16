package me.kiril.mesos.example;

import static com.mesosphere.mesos.rx.java.SinkOperations.sink;
import static com.mesosphere.mesos.rx.java.protobuf.SchedulerCalls.decline;
import static com.mesosphere.mesos.rx.java.protobuf.SchedulerCalls.subscribe;
import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.CommandInfo;
import org.apache.mesos.v1.Protos.Environment;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.OfferID;
import org.apache.mesos.v1.Protos.Resource;
import org.apache.mesos.v1.Protos.TaskID;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.TaskState;
import org.apache.mesos.v1.Protos.TaskStatus;
import org.apache.mesos.v1.Protos.Value;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.mesos.rx.java.SinkOperation;
import com.mesosphere.mesos.rx.java.SinkOperations;
import com.mesosphere.mesos.rx.java.protobuf.ProtoUtils;
import com.mesosphere.mesos.rx.java.protobuf.ProtobufMessageCodecs;
import com.mesosphere.mesos.rx.java.protobuf.SchedulerCalls;

import io.reactivex.Observable;
import me.kiril.mesos.client.MesosClient;

public final class Sleepy {
	private static final Logger LOGGER = LoggerFactory.getLogger(Sleepy.class);

	/**
	 * <pre>
	 * {@code
	 * Usage: java -cp <application-jar> com.mesosphere.mesos.rx.java.example.framework.sleepy.Sleepy <mesos-uri> <cpus-per-task> <mesos-resource-role>
	 * mesos-uri            The fully qualified URI to the Mesos Master. (http://localhost:5050/api/v1/scheduler)
	 * cpus-per-task        The number of CPUs each task should claim from an offer.
	 * mesos-resources-role The resource role to use when registering with mesos and evaluating offers
	 * }
	 * </pre>
	 * 
	 * @param args
	 *            Application arguments {@code mesos-uri}, {@code cpus-per-task}
	 *            and {@code mesos-resource-role}.
	 */
	public static void main(final String... args) {
		try {
			_main("sleepy-" + UUID.randomUUID(), args);
		} catch (Throwable e) {
			LOGGER.error("Unhandled exception caught at main", e);
			System.exit(1);
		}
	}

	static Observable<State<FrameworkID, TaskID, TaskState>> stateObservable;

	static void _main(final String fwId, final String... args) throws Throwable {
		if (args.length != 3) {
			final String className = Sleepy.class.getCanonicalName();
			System.err.println("Usage: java -cp <application-jar> " + className
					+ " <mesos-uri> <cpus-per-task> <mesos-resource-role>");
			System.exit(1);
		}

		final double cpusPerTask = Double.parseDouble(args[1]);
		final String role = args[2];
		final FrameworkID frameworkID = FrameworkID.newBuilder().setValue(fwId).build();
		final State<FrameworkID, TaskID, TaskState> stateObject = new State<>(frameworkID, role.trim(), cpusPerTask,
				16);


		final Call subscribeCall = subscribe(stateObject.getFwId(),
				Protos.FrameworkInfo.newBuilder().setId(stateObject.getFwId())
						.setUser(Optional.ofNullable(System.getenv("user")).orElse("root")) // https://issues.apache.org/jira/browse/MESOS-3747
						.setName("sleepy").setFailoverTimeout(0).setRole(stateObject.getResourceRole()).build());

		stateObservable = Observable.just(stateObject).repeat();


		final MesosClient<Call, Event> client = new MesosClient<Call, Event>(args[0],
				ProtobufMessageCodecs.SCHEDULER_CALL, ProtobufMessageCodecs.SCHEDULER_EVENT, Sleepy::streamProcessor);
		client.setSubscribe(subscribeCall);

		client.openStream();

		Thread.sleep(100_000_000);
	}

	public static Observable<Optional<SinkOperation<Call>>> streamProcessor(Observable<Event> unicastEvents)
			throws Exception {
		final Observable<Event> events = unicastEvents.share();

		final Observable<Optional<SinkOperation<Call>>> offerEvaluations = events
				.filter(event -> event.getType() == Event.Type.OFFERS)
				.flatMap(event -> Observable.fromIterable(event.getOffers().getOffersList()))
				.zipWith(stateObservable, Tuple2::create).map(Sleepy::handleOffer).map(Optional::of);

		final Observable<Optional<SinkOperation<Call>>> updateStatusAck = events
				.filter(event -> event.getType() == Event.Type.UPDATE && event.getUpdate().getStatus().hasUuid())
				.zipWith(stateObservable, Tuple2::create)
				.doOnNext((Tuple2<Event, State<FrameworkID, TaskID, TaskState>> t) -> {
					final Event event = t._1;
					final State<FrameworkID, TaskID, TaskState> state = t._2;
					final TaskStatus status = event.getUpdate().getStatus();
					state.put(status.getTaskId(), status.getState());
				}).map((Tuple2<Event, State<FrameworkID, TaskID, TaskState>> t) -> {
					final TaskStatus status = t._1.getUpdate().getStatus();
					return SchedulerCalls.ackUpdate(t._2.getFwId(), status.getUuid(), status.getAgentId(),
							status.getTaskId());
				}).map(SinkOperations::create).map(Optional::of);

		final Observable<Optional<SinkOperation<Call>>> errorLogger = events
				.filter(event -> event.getType() == Event.Type.ERROR || (event.getType() == Event.Type.UPDATE
						&& event.getUpdate().getStatus().getState() == TaskState.TASK_ERROR))
				.doOnNext(e -> LOGGER.warn("Task Error: {}", ProtoUtils.protoToString(e))).map(e -> Optional.empty());

		return offerEvaluations.mergeWith(updateStatusAck).mergeWith(errorLogger);
	}

	@NotNull
	private static SinkOperation<Call> handleOffer(
			@NotNull final Tuple2<Offer, State<FrameworkID, TaskID, TaskState>> t) {
		final Offer offer = t._1;
		final State<FrameworkID, TaskID, TaskState> state = t._2;
		final int offerCount = state.getOfferCounter().incrementAndGet();
		final String desiredRole = state.getResourceRole();

		final FrameworkID frameworkId = state.getFwId();
		final AgentID agentId = offer.getAgentId();
		final List<OfferID> ids = Arrays.asList(offer.getId());

		final Map<String, List<Resource>> resources = offer.getResourcesList().stream()
				.collect(groupingBy(Resource::getName));
		final List<Resource> cpuList = resources.get("cpus");
		final List<Resource> memList = resources.get("mem");
		if (cpuList != null && !cpuList.isEmpty() && memList != null && !memList.isEmpty()
				&& cpuList.size() == memList.size()) {
			final List<TaskInfo> tasks = new ArrayList<>();
			for (int i = 0; i < cpuList.size(); i++) {
				final Resource cpus = cpuList.get(i);
				final Resource mem = memList.get(i);

				if (desiredRole.equals(cpus.getRole()) && desiredRole.equals(mem.getRole())) {
					double availableCpu = cpus.getScalar().getValue();
					double availableMem = mem.getScalar().getValue();
					final double cpusPerTask = state.getCpusPerTask();
					final double memMbPerTask = state.getMemMbPerTask();
					while (availableCpu >= cpusPerTask && availableMem >= memMbPerTask) {
						availableCpu -= cpusPerTask;
						availableMem -= memMbPerTask;
						final String taskId = String.format("task-%d-%d", offerCount,
								state.getTotalTaskCounter().incrementAndGet());
						tasks.add(sleepTask(agentId, taskId, cpus.getRole(), cpusPerTask, mem.getRole(), memMbPerTask));
					}
				}
			}

			if (!tasks.isEmpty()) {
				LOGGER.info("Launching {} tasks", tasks.size());
				return sink(sleep(frameworkId, ids, tasks),
						() -> tasks.forEach(task -> state.put(task.getTaskId(), TaskState.TASK_STAGING)),
						(e) -> LOGGER.warn("", e));
			} else {
				return sink(decline(frameworkId, ids));
			}
		} else {
			return sink(decline(frameworkId, ids));
		}
	}

	@NotNull
	private static Call sleep(@NotNull final FrameworkID frameworkId, @NotNull final List<OfferID> offerIds,
			@NotNull final List<TaskInfo> tasks) {
		return Call.newBuilder().setFrameworkId(frameworkId).setType(Call.Type.ACCEPT)
				.setAccept(
						Call.Accept.newBuilder().addAllOfferIds(offerIds)
								.addOperations(Offer.Operation.newBuilder().setType(Offer.Operation.Type.LAUNCH)
										.setLaunch(Offer.Operation.Launch.newBuilder().addAllTaskInfos(tasks))))
				.build();
	}

	@NotNull
	private static TaskInfo sleepTask(@NotNull final AgentID agentId, @NotNull final String taskId,
			@NotNull final String cpusRole, final double cpus, @NotNull final String memRole, final double mem) {
		final String sleepSeconds = Optional.ofNullable(System.getenv("SLEEP_SECONDS")).orElse("15");
		return TaskInfo.newBuilder().setName(taskId).setTaskId(TaskID.newBuilder().setValue(taskId)).setAgentId(agentId)
				.setCommand(CommandInfo.newBuilder()
						.setEnvironment(Environment.newBuilder().addVariables(
								Environment.Variable.newBuilder().setName("SLEEP_SECONDS").setValue(sleepSeconds)))
						.setValue("env | sort && sleep $SLEEP_SECONDS"))
				.addResources(Resource.newBuilder().setName("cpus").setRole(cpusRole).setType(Value.Type.SCALAR)
						.setScalar(Value.Scalar.newBuilder().setValue(cpus)))
				.addResources(Resource.newBuilder().setName("mem").setRole(memRole).setType(Value.Type.SCALAR)
						.setScalar(Value.Scalar.newBuilder().setValue(mem)))
				.build();
	}

}
