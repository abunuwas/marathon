package mesosphere.marathon
package raml

import mesosphere.marathon.Protos.HealthCheckDefinition
import mesosphere.marathon.core.health.{ MesosCommandHealthCheck, MesosHealthCheck, MesosHttpHealthCheck, MesosTcpHealthCheck }
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.state.{ ArgvList, Command, Executable }

import scala.concurrent.duration._

trait HealthCheckConversion {

  implicit val httpSchemeRamlReader: Reads[HttpScheme, HealthCheckDefinition.Protocol] = Reads {
    case HttpScheme.Http => HealthCheckDefinition.Protocol.MESOS_HTTP
    case HttpScheme.Https => HealthCheckDefinition.Protocol.MESOS_HTTPS
  }

  implicit val commandHealthCheckRamlReader: Reads[CommandHealthCheck, Executable] = Reads { commandHealthCheck =>
    commandHealthCheck.command match {
      case sc: ShellCommand => Command(sc.shell)
      case av: ArgvCommand => ArgvList(av.argv)
    }
  }

  implicit val healthCheckRamlReader: Reads[(PodDefinition, HealthCheck), MesosHealthCheck] = Reads { src =>
    val (pod, check) = src

    def portIndex(endpointName: String): Option[Int] = {
      val i = pod.endpoints.indexWhere(_.name == endpointName)
      if (i == -1) throw new IllegalStateException(s"endpoint named $endpointName not defined in pod ${pod.id}")
      else Some(i)
    }

    check match {
      case HealthCheck(Some(httpCheck), None, None, gracePeriod, interval, maxConFailures, timeout, delay) =>
        MesosHttpHealthCheck(
          gracePeriod = gracePeriod.seconds,
          interval = interval.seconds,
          timeout = timeout.seconds,
          maxConsecutiveFailures = maxConFailures,
          delay = delay.seconds,
          path = httpCheck.path,
          protocol = httpCheck.scheme.map(Raml.fromRaml(_)).getOrElse(HealthCheckDefinition.Protocol.HTTP),
          portIndex = portIndex(httpCheck.endpoint)
        )
      case HealthCheck(None, Some(tcpCheck), None, gracePeriod, interval, maxConFailures, timeout, delay) =>
        MesosTcpHealthCheck(
          gracePeriod = gracePeriod.seconds,
          interval = interval.seconds,
          timeout = timeout.seconds,
          maxConsecutiveFailures = maxConFailures,
          delay = delay.seconds,
          portIndex = portIndex(tcpCheck.endpoint)
        )
      case HealthCheck(None, None, Some(execCheck), gracePeriod, interval, maxConFailures, timeout, delay) =>
        MesosCommandHealthCheck(
          gracePeriod = gracePeriod.seconds,
          interval = interval.seconds,
          timeout = timeout.seconds,
          maxConsecutiveFailures = maxConFailures,
          delay = delay.seconds,
          command = Raml.fromRaml(execCheck)
        )
      case _ =>
        throw new IllegalStateException("illegal RAML HealthCheck: expected one of http, tcp or exec checks")
    }
  }
}

object HealthCheckConversion extends HealthCheckConversion
