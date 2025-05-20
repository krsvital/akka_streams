package homework

import akka._
import akka.actor.typed._
import akka.persistence.query._
import akka.stream.scaladsl._
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.ClosedShape
import akka.stream.alpakka.slick.scaladsl.Slick
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal

import homework.AkkaWriteSide._
import akka.stream.alpakka.slick.javadsl.SlickSession

case class Result(state: Double, offset: Long)

object Result {
  def empty = Result(0, 1)
}

case class AkkaReadSide(system: ActorSystem[NotUsed]){
    implicit val materializer: actor.ActorSystem = system.classicSystem
    implicit val session: SlickSession = SlickSession.forConfig("slick-postgres")

    materializer.registerOnTermination(session.close())

    val latestResult: Result = Result.empty
    
    var latestCalculatedResult = latestResult.state

    val startOffset = if (latestResult.offset == 1) 1 else latestResult.offset + 1
    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)
    
    def updateState(event: Any, seqNum: Long): Result = {
      event match {
        case Added(_, amount) =>
          latestCalculatedResult += amount
          println(s"---> Log from Added: $latestCalculatedResult")

          latestResult.copy(state = latestCalculatedResult, offset = seqNum)
        case Multiplied(_, amount) =>
          latestCalculatedResult *= amount
          println(s"---> Log from Multiplied: $latestCalculatedResult")

          latestResult.copy(state = latestCalculatedResult, offset = seqNum)
        case Divided(_, amount) =>
          latestCalculatedResult /= amount
          println(s"---> Log from Divided: $latestCalculatedResult")

          latestResult.copy(state = latestCalculatedResult, offset = seqNum)
      }
    }

    val graph = GraphDSL.create(){
      implicit builder: GraphDSL.Builder[NotUsed] =>

        import session.profile.api._
        import GraphDSL.Implicits._

        val input = builder.add(source)
        val stateUpdater = builder.add(Flow[EventEnvelope].map{ e => updateState(e.event, e.sequenceNr) })

        val localSaveOutput = builder.add(Sink.foreach[Result]{ r =>
            println(s"---> Local state updated to ${r}")
        })

        val dbSaveOutput = builder.add(
          Slick.sink[Result]{ result: Result => sqlu"update public.result set calculated_value = ${result.state}, write_side_offset = ${result.offset} where id = 1" }
        )

        val broadcast = builder.add(Broadcast[Result](2))

        input ~> stateUpdater ~> broadcast ~> localSaveOutput
        broadcast ~> dbSaveOutput

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
}