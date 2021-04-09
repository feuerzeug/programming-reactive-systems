/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester,id,elem) => root ! Insert(requester, id, elem)
    case Contains(requester,id,elem) => root ! Contains(requester,id,elem)
    case Remove(requester,id,elem) => root ! Remove(requester,id,elem)
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation =>  pendingQueue :+= op
     // println("enqueueing {}",op)
    //case gc: GC.type => pendingQueue.enqueue(gc)
    case CopyFinished => {
      //root ! PoisonPill
      root = newRoot

      for (op <- pendingQueue) root ! op
      pendingQueue = Queue.empty[Operation]
      context.become(normal)


    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, elem: Int) =>
      if (this.elem == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val subtree = if (elem < this.elem) Left else Right
        if (subtrees.contains(subtree)) subtrees(subtree) ! Insert(requester, id , elem) else {
          subtrees = subtrees + (subtree -> context.actorOf(props(elem, false)))
          requester ! OperationFinished(id)
        }
      }

    case Contains(requester,id,elem) =>
      if (this.elem == elem) {
        requester ! ContainsResult(id,!removed)
      } else {
        val subtree = if (elem < this.elem) Left else Right
        if (subtrees.contains(subtree)) subtrees(subtree) ! Contains(requester, id, elem) else requester ! ContainsResult(id,result = false)
      }

    case Remove(requester,id,elem) =>
      if (this.elem == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val subtree = if (elem < this.elem) Left else Right
        if (subtrees.contains(subtree)) subtrees(subtree) ! Remove(requester,id,elem) else requester ! OperationFinished(id)
      }


    case CopyTo(treeNode) =>
      context.become(copying(subtrees.values.toSet,removed))
      if (!removed) treeNode ! Insert(self, 0, elem) else self ! OperationFinished(0)
      subtrees.values.foreach(_ ! CopyTo(treeNode))

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef],insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => if (expected.isEmpty) {
      context.parent ! CopyFinished
      self ! PoisonPill
    } else context.become(copying(expected,true))

    case CopyFinished => if (!(expected-sender).isEmpty) context.become(copying(expected-sender,insertConfirmed)) else
    if (insertConfirmed) {
      context.parent ! CopyFinished
      self ! PoisonPill
    } else context.become(copying(Set.empty,insertConfirmed))
  }
}
