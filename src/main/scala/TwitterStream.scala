
import twitter4j._
import twitter4j.conf.{ConfigurationBuilder, Configuration}

object TwitterStream {

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey("IIDMuharpre31Sw6odpWCLlzO")
      .setOAuthConsumerSecret("iDKlI3AQnkwYQY7aVRc4rUY3lIroYuv5ezELi531nczLAD7xR5")
      .setOAuthAccessToken("101957427-aHySISw2IwX4y0h176or5I5mHOuRZlY1IeutNO2G")
      .setOAuthAccessTokenSecret("BMN3m7dEhFZAcTI447OXfm9KAEValv834REs6UzRp9XUl")
      .build()
    twitterConf
  }

  def getStream = new TwitterStreamFactory(getTwitterConf).getInstance()

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = cb(status)
    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }
}
