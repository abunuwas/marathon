package mesosphere.marathon
package core.pod

import mesosphere.marathon.plugin.ContainerSpec

import scala.collection.immutable.Map

case class MesosContainer(
  name: String,
  exec: Option[raml.MesosExec] = None,
  resources: raml.Resources,
  endpoints: scala.collection.immutable.Seq[raml.Endpoint] = Nil,
  image: Option[raml.Image] = None,
  env: Map[String, state.EnvVarValue] = Map.empty,
  user: Option[String] = None,
  healthCheck: Option[core.health.MesosHealthCheck] = None,
  volumeMounts: scala.collection.immutable.Seq[raml.VolumeMount] = Nil,
  artifacts: scala.collection.immutable.Seq[raml.Artifact] = Nil, //TODO(PODS): use FetchUri
  labels: Map[String, String] = Map.empty,
  lifecycle: Option[raml.Lifecycle] = None) extends ContainerSpec
