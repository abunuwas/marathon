package mesosphere.marathon.raml

import mesosphere.marathon.core.pod.{ MesosContainer, PodDefinition }

trait ContainerConversion extends HealthCheckConversion {
  implicit val containerRamlWrites: Writes[(PodDefinition, MesosContainer), PodContainer] = Writes { src =>
    val (pod, c) = src
    PodContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      environment = Raml.toRaml(c.env),
      user = c.user,
      healthCheck = c.healthCheck.map(check => Raml.toRaml((pod, check))),
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    )
  }

  implicit val containerRamlReads: Reads[PodContainer, MesosContainer] = Reads { c =>
    MesosContainer(
      name = c.name,
      exec = c.exec,
      resources = c.resources,
      endpoints = c.endpoints,
      image = c.image,
      env = Raml.fromRaml(c.environment),
      user = c.user,
      volumeMounts = c.volumeMounts,
      artifacts = c.artifacts,
      labels = c.labels,
      lifecycle = c.lifecycle
    // we can't do healthCheck here, it's a chicken-and-egg problem. we need the entire PodDefinition
    // in order to convert healthCheck, but we won't have what need in PodDefinition until we mostly convert
    // the container.
    )
  }
}

object ContainerConversion extends ContainerConversion
