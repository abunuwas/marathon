package mesosphere.marathon
package core.task.tracker.impl

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.Task.{ LocalVolumeId, Reservation }
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.stream._
import org.apache.mesos.{ Protos => MesosProtos }
import org.slf4j.LoggerFactory

/**
  * Converts between [[Task]] objects and their serialized representation MarathonTask.
  */
object TaskSerializer {

  def fromProto(proto: Protos.MarathonTask): Task = {

    def required[T](name: String, maybeValue: Option[T]): T = {
      maybeValue.getOrElse(throw new IllegalArgumentException(s"task[${proto.getId}]: $name must be set"))
    }

    def opt[T](
      hasAttribute: Protos.MarathonTask => Boolean, getAttribute: Protos.MarathonTask => T): Option[T] = {

      if (hasAttribute(proto)) {
        Some(getAttribute(proto))
      } else {
        None
      }
    }

    def agentInfo: Instance.AgentInfo = {
      Instance.AgentInfo(
        host = required("host", opt(_.hasHost, _.getHost)),
        agentId = opt(_.hasSlaveId, _.getSlaveId).map(_.getValue),
        attributes = proto.getAttributesList.toIndexedSeq
      )
    }

    def reservation: Option[Task.Reservation] = if (proto.hasReservation) {
      Some(ReservationSerializer.fromProto(proto.getReservation))
    } else None

    def maybeAppVersion: Option[Timestamp] = if (proto.hasVersion) Some(Timestamp(proto.getVersion)) else None

    val taskStatus = Task.Status(
      stagedAt = Timestamp(proto.getStagedAt),
      startedAt = if (proto.hasStartedAt) Some(Timestamp(proto.getStartedAt)) else None,
      mesosStatus = opt(_.hasStatus, _.getStatus),
      condition = TaskConditionSerializer.fromProto(proto.getCondition)
    )

    def hostPorts = proto.getPortsList.map(_.intValue())(collection.breakOut)

    def launchedTask: Option[Task.Launched] = {
      if (proto.hasStagedAt) {
        Some(
          Task.Launched(
            status = taskStatus,
            hostPorts = hostPorts
          )
        )
      } else {
        None
      }
    }

    constructTask(
      taskId = Task.Id(proto.getId),
      agentInfo = agentInfo,
      reservation,
      launchedTask,
      taskStatus,
      maybeAppVersion
    )
  }

  private[this] def constructTask(
    taskId: Task.Id,
    agentInfo: Instance.AgentInfo,
    reservationOpt: Option[Reservation],
    launchedOpt: Option[Task.Launched],
    taskStatus: Task.Status,
    maybeVersion: Option[Timestamp]): Task = {

    val runSpecVersion = maybeVersion.getOrElse {
      val log = LoggerFactory.getLogger(getClass)
      // TODO(PODS): we cannot default to something meaningful here because Reserved tasks have no runSpec version
      log.warn(s"$taskId has no version. Defaulting to Timestamp.zero")
      Timestamp.zero
    }

    (reservationOpt, launchedOpt) match {

      case (Some(reservation), Some(launched)) =>
        Task.LaunchedOnReservation(
          taskId, agentInfo, runSpecVersion, launched.status, launched.hostPorts, reservation)

      case (Some(reservation), None) =>
        Task.Reserved(taskId, agentInfo, reservation, taskStatus, runSpecVersion)

      case (None, Some(launched)) =>
        Task.LaunchedEphemeral(
          taskId, agentInfo, runSpecVersion, launched.status, launched.hostPorts)

      case (None, None) =>
        val msg = s"Unable to deserialize task $taskId, agentInfo=$agentInfo. It is neither reserved nor launched"
        throw SerializationFailedException(msg)
    }
  }

  def toProto(task: Task): Protos.MarathonTask = {
    val builder = Protos.MarathonTask.newBuilder()

    def setId(taskId: Task.Id): Unit = builder.setId(taskId.idString)
    def setAgentInfo(agentInfo: Instance.AgentInfo): Unit = {
      builder.setHost(agentInfo.host)
      agentInfo.agentId.foreach { agentId =>
        builder.setSlaveId(MesosProtos.SlaveID.newBuilder().setValue(agentId))
      }
      builder.addAllAttributes(agentInfo.attributes)
    }
    def setReservation(reservation: Task.Reservation): Unit = {
      builder.setReservation(ReservationSerializer.toProto(reservation))
    }
    def setLaunched(status: Task.Status, hostPorts: Seq[Int]): Unit = {
      builder.setStagedAt(status.stagedAt.toDateTime.getMillis)
      status.startedAt.foreach(startedAt => builder.setStartedAt(startedAt.toDateTime.getMillis))
      status.mesosStatus.foreach(status => builder.setStatus(status))
      builder.addAllPorts(hostPorts.map(Integer.valueOf))
    }
    def setVersion(appVersion: Timestamp): Unit = {
      builder.setVersion(appVersion.toString)
    }
    def setTaskCondition(condition: Condition): Unit = {
      builder.setCondition(TaskConditionSerializer.toProto(condition))
    }

    setId(task.taskId)
    setAgentInfo(task.agentInfo)
    setTaskCondition(task.status.condition)
    setVersion(task.runSpecVersion)

    task match {
      case launched: Task.LaunchedEphemeral =>
        setLaunched(launched.status, launched.hostPorts)

      case reserved: Task.Reserved =>
        setReservation(reserved.reservation)

      case launchedOnR: Task.LaunchedOnReservation =>
        setLaunched(launchedOnR.status, launchedOnR.hostPorts)
        setReservation(launchedOnR.reservation)
    }

    builder.build()
  }
}

object TaskConditionSerializer {

  import mesosphere._
  import mesosphere.marathon.core.condition.Condition._

  private val proto2model = Map(
    marathon.Protos.MarathonTask.Condition.Reserved -> Reserved,
    marathon.Protos.MarathonTask.Condition.Created -> Created,
    marathon.Protos.MarathonTask.Condition.Error -> Error,
    marathon.Protos.MarathonTask.Condition.Failed -> Failed,
    marathon.Protos.MarathonTask.Condition.Finished -> Finished,
    marathon.Protos.MarathonTask.Condition.Killed -> Killed,
    marathon.Protos.MarathonTask.Condition.Killing -> Killing,
    marathon.Protos.MarathonTask.Condition.Running -> Running,
    marathon.Protos.MarathonTask.Condition.Staging -> Staging,
    marathon.Protos.MarathonTask.Condition.Starting -> Starting,
    marathon.Protos.MarathonTask.Condition.Unreachable -> Unreachable,
    marathon.Protos.MarathonTask.Condition.Gone -> Gone,
    marathon.Protos.MarathonTask.Condition.Unknown -> Unknown,
    marathon.Protos.MarathonTask.Condition.Dropped -> Dropped
  )

  private val model2proto: Map[Condition, marathon.Protos.MarathonTask.Condition] =
    proto2model.map(_.swap)

  def fromProto(proto: Protos.MarathonTask.Condition): Condition = {
    proto2model.getOrElse(proto, throw SerializationFailedException(s"Unable to parse $proto"))
  }

  def toProto(taskCondition: Condition): Protos.MarathonTask.Condition = {
    model2proto.getOrElse(
      taskCondition,
      throw SerializationFailedException(s"Unable to serialize $taskCondition"))
  }
}

private[impl] object ReservationSerializer {

  object TimeoutSerializer {
    import Protos.MarathonTask.Reservation.State.{ Timeout => ProtoTimeout }
    import Task.Reservation.Timeout
    def fromProto(proto: ProtoTimeout): Timeout = {
      val reason: Timeout.Reason = proto.getReason match {
        case ProtoTimeout.Reason.RelaunchEscalationTimeout => Timeout.Reason.RelaunchEscalationTimeout
        case ProtoTimeout.Reason.ReservationTimeout => Timeout.Reason.ReservationTimeout
        case _ => throw SerializationFailedException(s"Unable to parse ${proto.getReason}")
      }

      Timeout(
        Timestamp(proto.getInitiated),
        Timestamp(proto.getDeadline),
        reason
      )
    }

    def toProto(timeout: Timeout): ProtoTimeout = {
      val reason = timeout.reason match {
        case Timeout.Reason.RelaunchEscalationTimeout => ProtoTimeout.Reason.RelaunchEscalationTimeout
        case Timeout.Reason.ReservationTimeout => ProtoTimeout.Reason.ReservationTimeout
      }
      ProtoTimeout.newBuilder()
        .setInitiated(timeout.initiated.toDateTime.getMillis)
        .setDeadline(timeout.deadline.toDateTime.getMillis)
        .setReason(reason)
        .build()
    }
  }

  object StateSerializer {
    import Protos.MarathonTask.Reservation.{ State => ProtoState }
    import Task.Reservation.State

    def fromProto(proto: ProtoState): State = {
      val timeout = if (proto.hasTimeout) Some(TimeoutSerializer.fromProto(proto.getTimeout)) else None
      proto.getType match {
        case ProtoState.Type.New => State.New(timeout)
        case ProtoState.Type.Launched => State.Launched
        case ProtoState.Type.Suspended => State.Suspended(timeout)
        case ProtoState.Type.Garbage => State.Garbage(timeout)
        case ProtoState.Type.Unknown => State.Unknown(timeout)
        case _ => throw SerializationFailedException(s"Unable to parse ${proto.getType}")
      }
    }

    def toProto(state: Task.Reservation.State): ProtoState = {
      val stateType = state match {
        case Task.Reservation.State.New(_) => Protos.MarathonTask.Reservation.State.Type.New
        case Task.Reservation.State.Launched => Protos.MarathonTask.Reservation.State.Type.Launched
        case Task.Reservation.State.Suspended(_) => Protos.MarathonTask.Reservation.State.Type.Suspended
        case Task.Reservation.State.Garbage(_) => Protos.MarathonTask.Reservation.State.Type.Garbage
        case Task.Reservation.State.Unknown(_) => Protos.MarathonTask.Reservation.State.Type.Unknown
      }
      val builder = Protos.MarathonTask.Reservation.State.newBuilder()
        .setType(stateType)
      state.timeout.foreach(timeout => builder.setTimeout(TimeoutSerializer.toProto(timeout)))
      builder.build()
    }
  }

  def fromProto(proto: Protos.MarathonTask.Reservation): Task.Reservation = {
    if (!proto.hasState) throw SerializationFailedException(s"Serialized resident task has no state: $proto")

    val state: Task.Reservation.State = StateSerializer.fromProto(proto.getState)
    val volumes = proto.getLocalVolumeIdsList.map {
      case LocalVolumeId(volumeId) => volumeId
      case invalid: String => throw SerializationFailedException(s"$invalid is no valid volumeId")
    }

    Reservation(volumes, state)
  }

  def toProto(reservation: Task.Reservation): Protos.MarathonTask.Reservation = {
    Protos.MarathonTask.Reservation.newBuilder()
      .addAllLocalVolumeIds(reservation.volumeIds.map(_.idString))
      .setState(StateSerializer.toProto(reservation.state))
      .build()
  }
}
