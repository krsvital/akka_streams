package homework

import akka._
import akka.actor.typed._
import akka.actor.typed.scaladsl._

import homework.AkkaWriteSide._

object HomeworkMain {
    def apply(): Behavior[NotUsed] =
        Behaviors.setup{
            ctx =>
                val writeAcorRef = ctx.spawn(AkkaWriteSide("001"), "Calc", Props.empty)
                writeAcorRef ! Add(10)
                writeAcorRef ! Multiply(2)
                writeAcorRef ! Divide(5)

        Behaviors.same
    }

    def main(args: Array[String]): Unit = {
        AkkaReadSide(ActorSystem(HomeworkMain(), "homework"))
    }
}