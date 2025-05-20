package homework

import akka.actor.typed.scaladsl._
import akka.actor.typed.Behavior
import akka.persistence.typed.scaladsl._
import akka.persistence.typed.PersistenceId

object AkkaWriteSide {
    sealed trait Command
    case class Add(amount: Double) extends Command
    case class Multiply(amount: Double) extends Command
    case class Divide(amount: Double) extends Command

    sealed trait Event
    case class Added(id: Long, amount: Double) extends Event
    case class Multiplied(id: Long, amount: Double) extends Event
    case class Divided(id: Long, amount: Double) extends Event

    final case class State(value: Double)
    {
      def add(amount: Double): State = copy(value = value + amount)
      def multiply(amount: Double): State = copy(value = value * amount)
      def divide(amount: Double): State = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }

    def handleCommand(persistenceId: String, state: State, command: Command, ctx: ActorContext[Command]): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toLong, amount)
          Effect
            .persist(added)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toLong, amount)
          Effect
            .persist(multiplied)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toLong, amount)
          Effect
            .persist(divided)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
    }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(id: String): Behavior[Command] =
      Behaviors.setup { ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = PersistenceId.ofUniqueId(id),
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }
  }