package mesosphere.marathon.api.v2

import java.net.URI
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.event.EventStream
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.api.v2.validation.PodsValidation
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType, RestResource }
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.pod.{ PodDefinition, PodManager }
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.raml.Pod
import mesosphere.marathon.state.PathId
import play.api.libs.json.Json

@Path("v2/pods")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class PodsResource @Inject() (
    val config: MarathonConf,
    val authenticator: Authenticator,
    val authorizer: Authorizer)(
    implicit
    podSystem: PodManager,
    eventBus: EventStream,
    mat: Materializer) extends RestResource with AuthResource {

  import PodsResource._
  implicit val podDefValidator = PodsValidation.podDefValidator(config.availableFeatures)

  // If we change/add/upgrade the notion of a Pod and can't do it purely in the internal model,
  // update the json first
  private def normalize(pod: Pod): Pod = identity(pod)
  // If we can normalize using the internal model, do that instead.
  private def normalize(pod: PodDefinition): PodDefinition = {
    if (pod.networks.exists(_.name.isEmpty)) {
      val networks = pod.networks.map { network =>
        if (network.name.isEmpty) {
          config.defaultNetworkName.get.fold(network) { name =>
            network.copy(name = Some(name))
          }
        } else {
          network
        }
      }
      pod.copy(networks = networks)
    } else {
      pod
    }
  }

  private def marshal(pod: Pod): String = Json.stringify(Json.toJson(pod))

  private def marshal(pod: PodDefinition): String = marshal(pod.asPodDef)

  private def unmarshal(bytes: Array[Byte]): Pod = {
    normalize(Json.parse(bytes).as[Pod])
  }

  /**
    * HEAD is used to determine whether some Marathon variant supports pods.
    *
    * Performs basic authentication checks, but none for authorization: there's
    * no sensitive data being returned here anyway.
    *
    * @return HTTP OK if pods are supported
    */
  @HEAD
  @Timed
  def capability(@Context req: HttpServletRequest): Response = authenticated(req) { _ =>
    ok()
  }

  @POST @Timed
  def create(
    body: Array[Byte],
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = {
    authenticated(req) { implicit identity =>
      withValid(unmarshal(body)) { podDef =>
        withAuthorization(CreateRunSpec, podDef) {
          val pod = normalize(PodDefinition(podDef, config.defaultNetworkName.get))
          val deployment = result(podSystem.create(pod, force))
          Events.maybePost(PodEvent(req.getRemoteAddr, req.getRequestURI, PodEvent.Created))

          Response.created(new URI(pod.id.toString))
            .header(DeploymentHeader, deployment.id)
            .entity(marshal(pod))
            .build()
        }
      }
    }
  }

  @PUT @Timed @Path("""{id:.+}""")
  def update(
    @PathParam("id") id: String,
    body: Array[Byte],
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    withValid(unmarshal(body)) { podDef =>
      if (id != podDef.id) {
        Response.status(Status.BAD_REQUEST).entity(
          s"""
            |{"message": "'$id' does not match definition's id ('${podDef.id}')" }
          """.stripMargin
        ).build()
      } else {
        withAuthorization(UpdateRunSpec, podDef) {
          val pod = normalize(PodDefinition(podDef, config.defaultNetworkName.get))
          val deployment = result(podSystem.update(pod, force))
          Events.maybePost(PodEvent(req.getRemoteAddr, req.getRequestURI, PodEvent.Updated))

          val builder = Response
            .ok(new URI(pod.id.toString))
            .entity(marshal(pod))
            .header(DeploymentHeader, deployment.id)
          builder.build()
        }
      }
    }
  }

  @GET @Timed
  def findAll(@Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val pods = result(podSystem.findAll(isAuthorized(ViewRunSpec, _)).runWith(Sink.seq))
    ok(Json.stringify(Json.toJson(pods.map(_.asPodDef))))
  }

  @GET @Timed @Path("""{id:.+}""")
  def find(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    withValid(PathId(id)) { id =>
      result(podSystem.find(id)).fold(notFound(s"""{"message": "pod with $id does not exist"}""")) { pod =>
        withAuthorization(ViewRunSpec, pod) {
          ok(marshal(pod))
        }
      }
    }
  }

  @DELETE @Timed @Path("""{id:.+}""")
  def remove(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    withValid(PathId(id)) { id =>
      val pod = result(podSystem.find(id))
      withAuthorization(DeleteRunSpec, pod) {

        val deployment = result(podSystem.delete(id, force))

        Events.maybePost(PodEvent(req.getRemoteAddr, req.getRequestURI, PodEvent.Deleted))
        Response.status(Status.ACCEPTED)
          .location(new URI(deployment.id))
          .header(DeploymentHeader, deployment.id)
          .build()
      }
    }
  }

  @GET
  @Timed
  @Path("""{id:.+}::status""")
  def status(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    withValid(PathId(id)) { id =>
      val pod = result(podSystem.find(id))
      withAuthorization(ViewRunSpec, pod) {
        result(podSystem.status(id)).fold(notFound(id)) { status =>
          ok(Json.stringify(Json.toJson(status)))
        }
      }
    }
  }

  private def notFound(id: PathId): Response = notFound(s"""{"message": "pod '$id' does not exist"}""")
}

object PodsResource {
  val DeploymentHeader = "X-Marathon-Deployment-Id"
}
