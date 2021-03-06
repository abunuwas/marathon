package mesosphere.marathon.core.task.update.impl

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.{ TestInstanceBuilder, TestTaskBuilder }
import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.bus.TaskStatusUpdateTestHelper
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.PathId
import mesosphere.marathon.test.{ MarathonSpec, Mockito }
import org.apache.mesos.SchedulerDriver
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

class TaskStatusUpdateProcessorImplTest
    extends MarathonSpec with Mockito with ScalaFutures with GivenWhenThen with Matchers {

  for {
    (origUpdate, name) <- Seq(
      (TaskStatusUpdateTestHelper.finished(), "finished"),
      (TaskStatusUpdateTestHelper.error(), "error"),
      (TaskStatusUpdateTestHelper.killed(), "killed"),
      (TaskStatusUpdateTestHelper.killing(), "killing"),
      (TaskStatusUpdateTestHelper.failed(), "failed")
    )
  } {
    test(s"process update for unknown task that's $name will result in a noop") {
      fOpt = Some(new Fixture)
      val status = origUpdate.status
      val update = origUpdate
      val taskId = update.operation.instanceId

      Given("an unknown task")
      f.taskTracker.instance(taskId) returns Future.successful(None)

      When("we process the updated")
      f.updateProcessor.publish(status).futureValue

      Then("we expect that the appropriate taskTracker methods have been called")
      verify(f.taskTracker).instance(taskId)

      And("no kill is issued")
      noMoreInteractions(f.killService)

      And("the update has been acknowledged")
      verify(f.schedulerDriver).acknowledgeStatusUpdate(status)

      And("that's it")
      f.verifyNoMoreInteractions()
    }
  }

  test("process update for unknown task active task that's not lost will result in a kill and ack") {
    fOpt = Some(new Fixture)
    val taskToUpdate = TaskStatusUpdateTestHelper.defaultInstance
    val origUpdate = TaskStatusUpdateTestHelper.running(taskToUpdate)
    val status = origUpdate.status
    val update = origUpdate
    val instanceId = update.operation.instanceId

    Given("an unknown task")
    f.taskTracker.instance(instanceId) returns Future.successful(None)

    When("we process the updated")
    f.updateProcessor.publish(status).futureValue

    Then("we expect that the appropriate taskTracker methods have been called")
    verify(f.taskTracker).instance(instanceId)

    And("the task kill gets initiated")
    verify(f.killService).killUnknownTask(taskToUpdate.tasks.head.taskId, KillReason.Unknown)
    And("the update has been acknowledged")
    verify(f.schedulerDriver).acknowledgeStatusUpdate(status)

    And("that's it")
    f.verifyNoMoreInteractions()
  }

  // TODO: it should be up to the Task.update function to determine whether the received update makes sense
  // if not, a reconciliation should be triggered. Before, Marathon killed those tasks
  ignore("process update for known task without launchedTask that's not lost will result in a kill and ack") {
    fOpt = Some(new Fixture)
    val appId = PathId("/app")
    val instance = TestInstanceBuilder.newBuilder(appId).addTaskReserved(Task.Reservation(Iterable.empty, TestTaskBuilder.Helper.taskReservationStateNew)).getInstance()
    val origUpdate = TaskStatusUpdateTestHelper.finished(instance) // everything != lost is handled in the same way
    val status = origUpdate.status

    Given("an unknown task")
    f.taskTracker.instance(origUpdate.operation.instanceId) returns Future.successful(Some(instance))
    f.taskTracker.instance(any) returns {
      println("WTF")
      Future.successful(None)
    }

    When("we process the updated")
    f.updateProcessor.publish(status).futureValue

    Then("we expect that the appropriate taskTracker methods have been called")
    verify(f.taskTracker).instance(instance.instanceId)

    And("the task kill gets initiated")
    verify(f.killService).killInstance(instance, KillReason.Unknown)
    And("the update has been acknowledged")
    verify(f.schedulerDriver).acknowledgeStatusUpdate(status)

    And("that's it")
    f.verifyNoMoreInteractions()
  }

  test("TASK_KILLING is processed like a normal StatusUpdate") {
    fOpt = Some(new Fixture)

    val instance = TestInstanceBuilder.newBuilder(appId).addTaskRunning().getInstance()
    val origUpdate = TaskStatusUpdateTestHelper.killing(instance)
    val status = origUpdate.status
    val expectedTaskStateOp = InstanceUpdateOperation.MesosUpdate(instance, status, f.clock.now())

    Given("a task")
    f.taskTracker.instance(instance.instanceId) returns Future.successful(Some(instance))
    f.stateOpProcessor.process(expectedTaskStateOp) returns Future.successful(InstanceUpdateEffect.Update(instance, Some(instance), events = Nil))

    When("receive a TASK_KILLING update")
    f.updateProcessor.publish(status).futureValue

    Then("the task is loaded from the taskTracker")
    verify(f.taskTracker).instance(instance.instanceId)

    And("a MesosStatusUpdateEvent is passed to the stateOpProcessor")
    verify(f.stateOpProcessor).process(expectedTaskStateOp)

    And("the update has been acknowledged")
    verify(f.schedulerDriver).acknowledgeStatusUpdate(status)

    And("that's it")
    f.verifyNoMoreInteractions()

  }

  var fOpt: Option[Fixture] = None
  def f = fOpt.get

  lazy val appId = PathId("/app")

  after {
    fOpt.foreach(_.shutdown())
  }

  class Fixture {
    implicit lazy val actorSystem: ActorSystem = ActorSystem()
    lazy val clock: ConstantClock = ConstantClock()

    lazy val taskTracker: InstanceTracker = mock[InstanceTracker]
    lazy val stateOpProcessor: TaskStateOpProcessor = mock[TaskStateOpProcessor]
    lazy val schedulerDriver: SchedulerDriver = mock[SchedulerDriver]
    lazy val killService: KillService = mock[KillService]
    lazy val marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder = {
      val holder = new MarathonSchedulerDriverHolder
      holder.driver = Some(schedulerDriver)
      holder
    }

    lazy val updateProcessor = new TaskStatusUpdateProcessorImpl(
      new Metrics(new MetricRegistry),
      clock,
      taskTracker,
      stateOpProcessor,
      marathonSchedulerDriverHolder,
      killService,
      eventStream = actorSystem.eventStream
    )

    def verifyNoMoreInteractions(): Unit = {
      noMoreInteractions(taskTracker)
      noMoreInteractions(schedulerDriver)

      shutdown()
    }

    def shutdown(): Unit = {
      Await.result(actorSystem.terminate(), Duration.Inf)
      fOpt = None
    }
  }
}
