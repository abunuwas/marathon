package mesosphere.marathon.integration.setup

import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.setup.ProcessKeeper.MesosConfig
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state.{ AppDefinition, Container }
import org.scalatest.{ BeforeAndAfter, ConfigMap, GivenWhenThen, Matchers }

class MesosAppIntegrationTest
    extends IntegrationFunSuite
    with SingleMarathonIntegrationTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  // Configure Mesos to provide the Mesos containerizer with Docker image support.
  override def startMesos(): Unit = {
    ProcessKeeper.startMesosLocal(MesosConfig(
      port = config.mesosPort,
      launcher = "linux",
      containerizers = "mesos",
      isolation = Some("filesystem/linux,docker/runtime"),
      imageProviders = Some("docker")))
  }

  override def beforeAll(configMap: ConfigMap): Unit = {
    // Integration tests using docker image provisioning with the Mesos containerizer need to be
    // run as root in a Linux environment. They have to be explicitly enabled through an env variable.
    if (sys.env.getOrElse("RUN_MESOS_INTEGRATION_TESTS", "false") != "true") {
      cancel("Skipping this test suite because Mesos containerizer integration tests have been disabled")
    }

    super.beforeAll(configMap)
  }

  //clean up state before running the test case
  before(cleanUp())

  test("deploy a simple Docker app using the Mesos containerizer") {
    Given("a new Docker app")
    val app = AppDefinition(
      id = testBasePath / "mesosdockerapp",
      cmd = Some("sleep 600"),
      container = Some(Container.MesosDocker(image = "busybox")),
      resources = raml.Resources(cpus = 0.2, mem = 16.0),
      instances = 1
    )

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be(201) // Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1) // The app has really started
  }
}
