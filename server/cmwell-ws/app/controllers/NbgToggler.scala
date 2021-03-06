package controllers

import cmwell.ws.Settings
import com.typesafe.scalalogging.LazyLogging
import k.grid.dmap.api.SettingsBoolean
import k.grid.dmap.impl.persistent.PersistentDMap
import play.api.mvc.{Action, Controller, InjectedController}
import security.AuthUtils
import javax.inject._

import cmwell.ws.util.TypeHelpers
import filters.Attrs

@Singleton
class NbgController  @Inject()(nbgToggler: NbgToggler, authUtils: AuthUtils) extends InjectedController with LazyLogging with TypeHelpers {

  def handleToggle = Action { implicit req =>
    val tokenOpt = authUtils.extractTokenFrom(req)
    if (authUtils.isOperationAllowedForUser(security.Admin, tokenOpt, req.attrs(Attrs.Nbg))) {
      val nbg = !nbgToggler.get
      nbgToggler.set(nbg)
      Ok(s"Changed nbg flag to[$nbg]")
    }
    else Forbidden("not authorized")
  }
}

@Singleton
class NbgToggler  @Inject() extends LazyLogging with TypeHelpers {

  val NBG = "cmwell.ws.nbg"

  def set(b: Boolean) = PersistentDMap.set(NBG, SettingsBoolean(b))

  def get: Boolean = PersistentDMap.get(NBG).fold[Boolean](Settings.nbgToggler) {
    case SettingsBoolean(v) => v
    case unknown => throw new IllegalStateException(s"invalid unknown state[cmwell.ws.nbg]: $unknown")
  }
}
