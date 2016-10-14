package mesosphere.marathon.core.task

import mesosphere.UnitTest
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.{InstanceStatus, TestTaskBuilder}
import mesosphere.marathon.core.task.bus.MesosTaskStatusTestHelper
import mesosphere.marathon.core.task.update.{TaskUpdateEffect, TaskUpdateOperation}
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos.TaskState

import scala.concurrent.duration._

class TaskLaunchedEphemeralTest extends UnitTest {

  "LaunchedEphemeral" when {
    "updating a running task with a TASK_UNREACHABLE" should {
      val f = new Fixture

      val task = TestTaskBuilder.Helper.minimalRunning(appId = f.appId, since = f.clock.now)

      f.clock += 5.seconds

      val status = MesosTaskStatusTestHelper.unreachable(task.taskId, f.clock.now)
      val update = TaskUpdateOperation.MesosUpdate(MarathonTaskStatus(status), status, f.clock.now)

      val effect = task.update(update)

      "result in an update"  in { effect shouldBe a[TaskUpdateEffect.Update] }
      "update to unreachable task status" in {
        val newStatus = effect.asInstanceOf[TaskUpdateEffect.Update].newState.status.mesosStatus.get.getState
        newStatus should be(TaskState.TASK_UNREACHABLE)
      }
      "update to unreachable instance status" in {
        val newStatus = effect.asInstanceOf[TaskUpdateEffect.Update].newState.status.taskStatus
          newStatus should be(InstanceStatus.Unreachable)
      }
    }

    "updating a running task with a TASK_UNREACHABLE that is older than 15 minutes" should {
      val f = new Fixture

      val task = TestTaskBuilder.Helper.minimalRunning(appId = f.appId, since = f.clock.now)

      f.clock += 5.seconds

      val status = MesosTaskStatusTestHelper.unreachable(task.taskId, f.clock.now)

      // 16 minutes pass so the status happened more than 15 minutes ago.
      f.clock += 16.minutes

      val update = TaskUpdateOperation.MesosUpdate(MarathonTaskStatus(status), status, f.clock.now)

      val effect = task.update(update)

      "result in an update"  in { effect shouldBe a[TaskUpdateEffect.Update] }
      "update to unreachable task status" in {
        val newStatus = effect.asInstanceOf[TaskUpdateEffect.Update].newState.status.mesosStatus.get.getState
        newStatus should be(TaskState.TASK_UNREACHABLE)
      }
      "update to unknown instance state" in {
        val newStatus = effect.asInstanceOf[TaskUpdateEffect.Update].newState.status.taskStatus
        newStatus should be(InstanceStatus.Unknown)
      }
    }
  }

  class Fixture {
    val appId = PathId("/app")
    val clock = ConstantClock()
  }
}
