package mesosphere.marathon
package raml

import mesosphere.marathon.Protos.HealthCheckDefinition
import mesosphere.marathon.core.health.{ MesosCommandHealthCheck, MesosHealthCheck, MesosHttpHealthCheck, MesosTcpHealthCheck }
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

  implicit val healthCheckRamlReader: Reads[HealthCheck, MesosHealthCheck] = Reads {
    case HealthCheck(Some(httpCheck), None, None, gracePeriodSec, intervalSec, maxConFailures, timeoutSec, delaySec) =>
      MesosHttpHealthCheck(
        gracePeriod = gracePeriodSec.seconds,
        interval = intervalSec.seconds,
        timeout = timeoutSec.seconds,
        maxConsecutiveFailures = maxConFailures,
        delay = delaySec.seconds,
        path = httpCheck.path,
        protocol = httpCheck.scheme.map(scheme => Raml.fromRaml(scheme)).getOrElse(HealthCheckDefinition.Protocol.HTTP)
      // TODO(pods) set portName once MesosHttpHealthCheck supports it
      )
    case HealthCheck(None, Some(tcpCheck), None, gracePeriodSec, intervalSec, maxConFailures, timeoutSec, delaySec) =>
      MesosTcpHealthCheck(
        gracePeriod = gracePeriodSec.seconds,
        interval = intervalSec.seconds,
        timeout = timeoutSec.seconds,
        maxConsecutiveFailures = maxConFailures,
        delay = delaySec.seconds
      // TODO(pods) set portName once MesosHttpHealthCheck supports it
      )
    case HealthCheck(None, None, Some(execCheck), gracePeriodSec, intervalSec, maxConFailures, timeoutSec, delaySec) =>
      MesosCommandHealthCheck(
        gracePeriod = gracePeriodSec.seconds,
        interval = intervalSec.seconds,
        timeout = timeoutSec.seconds,
        maxConsecutiveFailures = maxConFailures,
        delay = delaySec.seconds,
        command = Raml.fromRaml(execCheck)
      )
    case _ =>
      throw new IllegalArgumentException("illegal RAML health check")
  }
}

object HealthCheckConversion extends HealthCheckConversion
