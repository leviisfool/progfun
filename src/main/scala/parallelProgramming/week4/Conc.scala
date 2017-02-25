package parallelProgramming.week4

import org.scalameter.{Key, Warmer, config}

import scala.annotation.tailrec
import scala.collection.parallel.Combiner
import scala.reflect.ClassTag

/**
  * Created by levi on 2/25/17.
  */
sealed trait Conc[T] {
  def level: Int
  def size: Int
  def left: Conc[T]
  def right: Conc[T]

  def <>(that: Conc[T]): Conc[T] = {
    if (this == Empty) that
    else if (that == Empty) this
    else concat(this, that)


  }

  def concat[T](xs: Conc[T], ys: Conc[T]): Conc[T] = {
    val diff = ys.level - xs.level
    if (diff >= -1 && diff <= 1) new <>(xs, ys)
    else {
      if (xs.left.level >= xs.right.level) {
        val nr = concat(xs.right, ys)
        new <>(xs.left, nr)
      } else {
        val nrr = concat(xs.right.right, ys)
        if (nrr.level == xs.level - 3) {
          val nl = xs.left
          val nr = new <>(xs.right.left, nrr)
          new <>(nl, nr)
        } else {
          val nl = new <>(xs.left, xs.right.left)
          val nr = nrr
          new <>(nl, nr)
        }
      }
    }
  }


}

case object Empty extends Conc[Nothing] {
  def level = 0

  def size = 0

  override def left: Conc[Nothing] = ???

  override def right: Conc[Nothing] = ???
}

case class Single[T](val x: T) extends Conc[T] {
  def level = 0

  def size = 1

  override def left: Conc[T] = ???

  override def right: Conc[T] = ???
}

case class <>[T](left: Conc[T], right: Conc[T]) extends Conc[T] {
  val level = 1 + math.max(left.level, right.level)
  val size = left.size + right.size

}

case class Append[T](left: Conc[T], right: Conc[T]) extends Conc[T] {
  val level = 1 + math.max(left.level, right.level)
  val size = left.size + right.size

  def appendLeaf[T](xs: Conc[T], y: T): Conc[T] = Append(xs, new Single(y))

  def appendLeaf[T](xs: Conc[T], ys: Single[T]): Conc[T] = xs match {
    case Empty => ys
    case xs: Single[T] => new <>(xs, ys)
    case _ <> _ => new Append(xs, ys)
    case xs: Append[T] => append(xs, ys)
  }

  @tailrec private def append[T](xs: Append[T], ys: Conc[T]): Conc[T] = {
    if (xs.right.level > ys.level) new Append(xs, ys)
    else {
      val zs = new <>(xs.right, ys)
      xs.left match {
        case ws @ Append(_, _) => append(ws, zs)
        case ws if ws.level <= zs.level => ws <> zs
        case ws => new Append(ws, zs)
      }
    }
  }
}

class ConcBuffer[T: ClassTag](val k: Int, private var conc: Conc[T]) {
  private var chunk: Array[T] = new Array(k)
  private var chunkSize: Int = 0

  final def +=(elem: T): ConcBuffer[T] = {
    if (chunkSize >= k) expand()
    chunk(chunkSize) = elem
    chunkSize += 1
    this
  }

  private def expand() {
    conc = appendLeaf(conc, new Chunk(chunk, chunkSize))
    chunk = new Array(k)
    chunkSize = 0
  }

  final def combine(that: ConcBuffer[T]): ConcBuffer[T] = {
    val combinedConc = this.result <> that.result
    new ConcBuffer(k, combinedConc)
  }

  def result: Conc[T] = {
    conc = appendLeaf(conc, new Chunk(chunk, chunkSize))
    conc
  }

  def appendLeaf(conc: Conc[T], chunk: Chunk[T]): Conc[T] = {
    new Append[T](conc, chunk)
  }

}

class Chunk[T](val array: Array[T], val size: Int) extends Conc[T] {
  def level = 0

  override def left: Conc[T] = ???

  override def right: Conc[T] = ???
}

object ConcBuffer {
  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 60,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]) {
    val size = 1000000

    def run(p: Int) {
      val taskSupport = new collection.parallel.ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(p))
      val strings = (0 until size).map(_.toString)
      val time = standardConfig measure {
        val parallelized = strings.par
        parallelized.tasksupport = taskSupport
        parallelized.aggregate(new ConcBuffer[String](100, new <>(new Single[String](""), new Single[String](""))))(_ += _, _ combine _).result
      }
      println(s"p = $p, time = $time ms")
    }

    run(1)
    run(2)
    run(4)
    run(8)
  }
}