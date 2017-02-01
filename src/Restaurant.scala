import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.runtime.universe._

import scala.collection.immutable.Queue

object Id {
  var a = new AtomicInteger(100)
  def next: Id = Id(a.getAndIncrement())
  def external: Id = Id(-1)
  def caused(id: Id): Id = Id(id.id)
}
case class Id(id: Int) {
  override def toString = "" + id
}

abstract class OrderMsg(
    val msgId: Id = Id.next,
    val corrId: Id,
    val causeId: Id
) {
  def order: Order
  def log(msg: String): Unit = {
    val tname = Thread.currentThread().getName
    val o = order
    Console.println(s"${tname}|${corrId}|m${msgId}|c${causeId}|o${o.id}|${msg}")
  }
}
case class Placed(order: Order)
    extends OrderMsg(corrId = Id.next, causeId = order.id)
case class CookFood(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class Cooked(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class BillOrder(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class Billed(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class TakePayment(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class Payed(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)
case class Dropped(causedBy: OrderMsg, order: Order)
    extends OrderMsg(corrId = causedBy.corrId, causeId = causedBy.msgId)

case class Order(
    id: Id,
    tableNumber: Int = 0,
    items: List[Item] = Nil,
    subTotal: Int = 0,
    tax: Int = 0,
    total: Int = 0,
    cookTime: Int = 0,
    ingredients: List[String] = Nil,
    payed: Boolean = false,
    starttime: Long = System.currentTimeMillis(),
    TTL: Long = 500
) {
  def age = (System.currentTimeMillis() - starttime)
  def expired = age > TTL
}

case class Item(
    name: String,
    quantity: Int,
    price: Int
)

trait Handles[M] {
  def handle(order: M): Unit
}

object OrderPrinter extends Handles[Billed] {
  override def handle(order: Billed): Unit = {
    order.log("done") // order.toString
  }
}

case class Waiter(publisher: Publisher) {
  def placeOrder(wish: String): Id = {
    if ("cake" == wish) {
      val order = Order(Id.next)
      publisher.publish(Placed(order))
      order.id
    } else {
      throw new IllegalArgumentException("Unknown wish " + wish);
    }
  }
}

case class Cook(name: String, speed: Long, publisher: Publisher)
    extends Handles[CookFood] {
  override def handle(msg: CookFood): Unit = {
    val cake = List("water", "sugar", "butter")
    msg.log(s"$name is cooking ... $cake")
    Thread.sleep(speed)
    publisher.publish(Cooked(msg, msg.order.copy(ingredients = cake)))
  }
}

case class AssistantManager(publisher: Publisher) extends Handles[BillOrder] {
  override def handle(msg: BillOrder): Unit = {
    msg.log("Assistant Manager adding prices")
    Thread.sleep(500L)
    publisher.publish(
      Billed(msg, msg.order.copy(subTotal = 100, tax = 40, total = 140)))
  }
}

case class Cashier(publisher: Publisher) extends Handles[TakePayment] {
  var total = 0
  override def handle(msg: TakePayment): Unit = {
    msg.log(s"Cashier getting money. Total is now ${total}")
    Thread.sleep(500L)
    total = total + msg.order.total
    publisher.publish(Payed(msg, msg.order.copy(payed = true)))
  }
}

case class OrderInitializer(publisher: Publisher, managers: Managers) extends Handles[Placed] {
  override def handle(order: Placed): Unit = managers.start(order)
}

case class Managers(publisher: Publisher) extends Handles[OrderMsg] {
  var map : Map[Id, Manager] = Map()

  def start(order : Placed) : Unit = {
    PubSub.subscribe(order.corrId, this)
    val manager = if (order.corrId.id % 2 == 0) {
      new PayLast(publisher, () => done(order.corrId))
    } else {
      new PayFirst(publisher, () => done(order.corrId))
    }
    map = map.updated(order.corrId, manager)
    manager.handle(order)
  }

  def done(corrId : Id): Unit = {
    Console.println(s"removing manager for ${corrId}")
    map = map - corrId
  }

  override def handle(order: OrderMsg): Unit = {
    map.get(order.corrId) match {
      case Some(m) => m.handle(order)
      case None => order.log(s"No manager subscribed to this order ${order.corrId}")
    }
  }
}

trait Manager extends Handles[OrderMsg]

case class PayLast(publisher: Publisher, doneCallback : () => Unit) extends Manager {
  val id = Id.next
  override def handle(msg: OrderMsg): Unit = {
    msg match {
      case Placed(_) => publisher.publish(CookFood(msg, msg.order))
      case Cooked(_,_) => publisher.publish(BillOrder(msg, msg.order))
      case Billed(_,_) => publisher.publish(TakePayment(msg, msg.order))
      case Payed(_,_) => doneCallback()
      case Dropped(_,_) => {} // cry
      case _ =>  // ignore
    }
  }
}

case class PayFirst(publisher: Publisher, doneCallback : () => Unit) extends Manager {
  val id = Id.next
  override def handle(msg: OrderMsg): Unit = {
    msg match {
      case Placed(_) => publisher.publish(BillOrder(msg, msg.order))
      case Billed(_,_) => publisher.publish(TakePayment(msg, msg.order))
      case Payed(_,_) => publisher.publish(CookFood(msg, msg.order))
      case Cooked(_,_) => doneCallback()
      case Dropped(_,_) => {} // cry
      case _ =>  // ignore
    }
  }
}

case class Repeater(items: List[Handles[OrderMsg]]) extends Handles[OrderMsg] {
  override def handle(order: OrderMsg): Unit = {
    order.log("Repeating")
    for (next <- items) {
      next.handle(order)
    }
  }
}

//case class RoundRobin(items: Seq[ThreadedHandler]) extends Handles[OrderMsg] {
//  var queue : Queue[Handles[OrderMsg]] = Queue(items : _*)
//  override def handle(order: Order): Unit = {
//    order.log("Round robin")
//    val (next, rest) = queue.dequeue
//    queue = rest.enqueue(next)
//    next.handle(order)
//  }
//}

case class ThreadedHandler[M](name: String, next: Handles[M])
    extends Handles[M]
    with Runnable {
  var queue: scala.collection.mutable.Queue[M] =
    scala.collection.mutable.Queue()
  override def handle(msg: M): Unit = {
    val order = msg.asInstanceOf[OrderMsg].order
    queue.enqueue(msg)
//    msg
//      .asInstanceOf[OrderMsg]
//      .log(s"$name: Queued ${order.id.id}. Order queue is now ${queue.size}")
  }

  def count = queue.size

  def start(): ThreadedHandler[M] = {
    val thread = new Thread(this)
    thread.setName(name)
    // Console.println(s"Starting thread ${thread.getId}")
    thread.start()
    this
  }

  override def run(): Unit = {
    while (true) {
      if (queue.size > 0) {
        val msg = queue.dequeue
        val order = msg.asInstanceOf[OrderMsg].order
//        msg
//          .asInstanceOf[OrderMsg]
//          .log(
//            s"$name: Dequeued order ${order.id.id}. Order queue is now ${queue.size}")
        next.handle(msg)
      }
      Thread.sleep(1)
    }
  }
}

case class MoreFair[M](handlers: Seq[ThreadedHandler[M]]) extends Handles[M] {
  var queue: Queue[ThreadedHandler[M]] = Queue(handlers: _*)

  override def handle(msg: M): Unit = {
    while (true) {
      if (!queue.isEmpty) {
        val (next, rest) = queue.dequeue
        queue = rest.enqueue(next)
        if (next.count < 5) {
          next.handle(msg)
          return
        }
      }
      Thread.sleep(10)
    }
  }
}

case class ShortestQueue[M](handlers: Seq[ThreadedHandler[M]])
    extends Handles[M] {
  var list: List[ThreadedHandler[M]] = handlers.toList

  override def handle(msg: M): Unit = {
    while (true) {
      val next = list.minBy(_.count)
      if (next.count < 5) {
        next.handle(msg)
        return
      }
      Thread.sleep(10)
    }
  }
}

case class TTLChecker[M](next: Handles[M], publisher: Publisher)
    extends Handles[M] {
  override def handle(msg: M): Unit = {
    val omsg = msg.asInstanceOf[OrderMsg]
    val order = omsg.order
    if (order.expired) {
      omsg.log(s"Dropping order is is ${order.age}ms old")
      publisher.publish(Dropped(omsg, order))
    } else {
      next.handle(msg)
    }
  }
}

class Monitor[M](monitor: Seq[ThreadedHandler[M]]) extends Thread {
  val starttime = System.currentTimeMillis()
  override def run() = {
    Thread.currentThread().setName("Mon")
    while (true) {
      Thread.sleep(500)
      var total = (monitor.map(c => c.count).sum)
      Console.println("==== " + total + " orders in progress")
      for (c <- monitor) {
        Console.println(s"${c.name} has ${c.count} orders waiting")
      }
      if (total == 0) {
        Thread.sleep(10000)
        Console.println(
          s"Exit after ${System.currentTimeMillis() - starttime}ms")
        System.exit(1)
      }
    }
  }
}

class Customers(waiter: Waiter) extends Thread {
  override def run() = {
    Thread.currentThread().setName("Cus")
    for (i <- 0 until 4) {
      waiter.placeOrder("cake")
    }
  }
}

trait Publisher {
  def publish[M <: OrderMsg](msg: M)(implicit tag: TypeTag[M]): Unit
}

object MsgPub extends Publisher {
  def publish[M <: OrderMsg](msg: M)(implicit tag: TypeTag[M]): Unit = {
    val omsg = msg.asInstanceOf[OrderMsg]
    PubSub.publish(msg)
    PubSub.publish(omsg.corrId, msg)
  }
}

object PubSub {
  var topics: Map[String, List[Handles[_]]] = Map()

  def publish[M](msg: M)(implicit tag: TypeTag[M]): Unit = {
    // this and the implicit is magic to get the type of M
    val topic = tag.tpe.toString

    publish(topic, msg)
  }

  def publish[M](corrId: Id, msg: M): Unit = {
    publish(corrId.toString, msg)
  }

  def publish[M](topic: String, msg: M): Unit = {
    val handlers = topics.getOrElse(topic, Nil)
    for (handler <- handlers) {
      //handler.handle(msg)
      val handleMethod =
        handler.getClass.getMethods.find(m => m.getName() == "handle").get
      handleMethod.invoke(handler, msg.asInstanceOf[Object])
    }
  }

  def subscribe[M](handler: Handles[M])(implicit tag: TypeTag[M]): Unit = {
    val topic = tag.tpe.toString
    subscribe(topic, handler)
  }

  def subscribe[M](corrId: Id, handler: Handles[M]): Unit = {
    subscribe(corrId.toString, handler)
  }

  def subscribe[M](topic: String, handler: Handles[M]): Unit = {
    val subs = handler :: topics.getOrElse(topic, Nil)
    topics = topics.updated(topic, subs)
  }
}

object Main extends App {
  Thread.currentThread().setName("main")
  // construction
  val managers = Managers(MsgPub)
  val orderInitializer = ThreadedHandler("Mgr", OrderInitializer(MsgPub, managers))
  val cashier = ThreadedHandler("Cas", Cashier(MsgPub))
  val assistantMgr = ThreadedHandler("Ass", AssistantManager(MsgPub))
  def cook(name: String, speed: Long) =
    ThreadedHandler(name, TTLChecker(Cook(name, speed, MsgPub), MsgPub))
  val cooks = Seq(cook("Bob", 5000), cook("Tom", 500), cook("Jim", 300))
  private val kitchen = ThreadedHandler("Chf", ShortestQueue(cooks))
  val waiter = Waiter(MsgPub)
  val customers = new Customers(waiter)

  // subscription
  PubSub.subscribe(orderInitializer)
  PubSub.subscribe(kitchen)
  PubSub.subscribe(assistantMgr)
  PubSub.subscribe(cashier)
  PubSub.subscribe(OrderPrinter)

  // start
  // new Monitor(cooks).start()
  orderInitializer.start()
  cashier.start()
  assistantMgr.start()
  cooks.map(_.start())
  kitchen.start()
  customers.start()
}
