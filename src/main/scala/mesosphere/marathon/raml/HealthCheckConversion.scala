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

  implicit val httpSchemeRamlWriter: Writes[HealthCheckDefinition.Protocol, HttpScheme] = Writes {
    case HealthCheckDefinition.Protocol.MESOS_HTTP => HttpScheme.Http
    case HealthCheckDefinition.Protocol.MESOS_HTTPS => HttpScheme.Https
    case p => throw new IllegalArgumentException(s"cannot convert health-check protocol $p to raml")
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

  implicit val healthCheckRamlWriter: Writes[(PodDefinition, MesosHealthCheck), HealthCheck] = Writes { src =>
    val (pod, check) = src
    def requiredField[T](msg: String): T = throw new IllegalStateException(msg)
    def endpointAt(portIndex: Option[Int]): String = portIndex
      .fold(requiredField[String](s"portIndex missing for health-check with pod ${pod.id}")){ idx =>
        pod.endpoints(idx).name
      }
    val partialCheck = HealthCheck(
      gracePeriodSeconds = check.gracePeriod.toSeconds,
      intervalSeconds = check.interval.toSeconds.toInt,
      maxConsecutiveFailures = check.maxConsecutiveFailures,
      timeoutSeconds = check.timeout.toSeconds.toInt,
      delaySeconds = check.delay.toSeconds.toInt
    )
    check match {
      case httpCheck: MesosHttpHealthCheck =>
        partialCheck.copy(
          http = Some(HttpHealthCheck(
            endpoint = endpointAt(httpCheck.portIndex),
            path = httpCheck.path,
            scheme = Some(Raml.toRaml(httpCheck.protocol))))
        )
      case tcpCheck: MesosTcpHealthCheck =>
        partialCheck.copy(
          tcp = Some(TcpHealthCheck(
            endpoint = endpointAt(tcpCheck.portIndex)
          ))
        )
      case cmdCheck: MesosCommandHealthCheck =>
        partialCheck.copy(
          exec = Some(CommandHealthCheck(
            command = cmdCheck.command match {
              case cmd: state.Command => ShellCommand(cmd.value)
              case argv: state.ArgvList => ArgvCommand(argv.value)
            }
          ))
        )
    }
  }
}

object HealthCheckConversion extends HealthCheckConversion
