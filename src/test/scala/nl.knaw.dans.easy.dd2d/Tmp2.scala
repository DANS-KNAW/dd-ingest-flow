package nl.knaw.dans.easy.dd2d

import nl.knaw.dans.lib.dataverse.model.user.AuthenticatedUser
import nl.knaw.dans.lib.dataverse.{ AdminApi, DataverseClient, DataverseResponse }

import java.util.function.Consumer

class Tmp2 extends TestSupportFixture {
  import java.io.File
  import java.util.function._
  import scala.compat.java8.FunctionConverters._

  val str = "fish"
  val fyl = new File("salmon")
  val num = 42
  val nmm = 9L
  val nnn = 0.3

  var cache: Any = null
  def save(a: Any) = { cache = a; a }
  def recall = { val ans = cache; cache = null; ans }

  case class Box[A](value: A) {}

  def sameS[A,B,C,D,E,F](f: (A, B) => C, g: (D, E) => F)(implicit ev1: A =:= D, ev2: B =:= E, ev3: C =:= F): Box[(A,B) => Boolean] =
    Box((a: A, b: B) => f(a,b) == g(ev1(a),ev2(b)))

  def sameS[A,B,C,D](f: A => B, g: C => D)(implicit ev1: A =:= C, ev2: B =:= D): Box[A => Boolean] =
    Box((a: A) => f(a) == g(ev1(a)))

  // BiConsumer tests; conceptually widens to BiFunction, narrows to ObjLongConsumer
  "BiConsumer" should "" in {
    val bic1 = new BiConsumer[String, File]{ def accept(s: String, f: File) { save((s,f)) } }
    val bic2 = new BiConsumer[Int, Long]{ def accept(i: Int, l: Long) { save((i,l)) } }
    val sbic = (s: String, f: File) => { save((s,f)); () }
    val zbic = (i: Int, l: Long) => { save((i,l)); () }
    def jbic[A, B](bic: BiConsumer[A, B])(a: A, b: B) = { bic.accept(a,b); recall == (a,b) }
    def fbic[A, B](f: (A,B) => Unit)(a: A, b: B) = { f(a,b); recall == (a,b) }
    jbic(asJavaBiConsumer(sbic))(str, fyl) shouldBe true
    assert(jbic(asJavaBiConsumer(zbic))(num, nmm))
    assert(jbic(sbic.asJava)(str, fyl))
    // assert(jbic(zbic.asJava)(num, nmm))  --  ObjLongConsumer
    assert(fbic(asScalaFromBiConsumer(bic1))(str, fyl))
    assert(fbic(asScalaFromBiConsumer(bic2))(num, nmm))
    assert(fbic(bic1.asScala)(str, fyl))
    assert(fbic(bic2.asScala)(num, nmm))
  }
  // BiFunction tests; conceptually narrows to any of the Bi functions or to ObjLongConsumer etc
  "BiFunction" should "" in {
    val bif1 = new BiFunction[String, File, (String, File)]{ def apply(s: String, f: File) = (s,f) }
    val bif2 = new BiFunction[Int, Long, Double]{ def apply(i: Int, l: Long) = i.toDouble*l }
    val sbif = (s: String, f: File) => (s,f)
    val zbif = (i: Int, l: Long) => i.toDouble*l
    def sameJ[A,B,C,D,E,F](f: BiFunction[A, B, C], g: BiFunction[D, E, F])(implicit ev1: A =:= D, ev2: B =:= E, ev3: C =:= F) =
      Box((a: A, b: B) => {
        val c = f.apply(a, b)
        c == g.apply(ev1(a), ev2(b))
      }
      )
    assert(sameJ(bif1, sbif.asJava).value(str,fyl))
    assert(sameJ(bif1, asJavaBiFunction(sbif)).value(str,fyl))
    // assert(sameJ(bif2, zbif.asJava))  -- ToDoubleBiFunction
    assert(sameJ(bif2, asJavaBiFunction(zbif)).value(num,nmm))
    assert(sameS(bif1.asScala, sbif).value(str,fyl))
    assert(sameS(asScalaFromBiFunction(bif1), sbif).value(str,fyl))
    assert(sameS(bif2.asScala, zbif).value(num,nmm))
    assert(sameS(asScalaFromBiFunction(bif2), zbif).value(num,nmm))
  }

  "test_BinaryOperator" should "" in {
    val bop1 = new BinaryOperator[String]{ def apply(s: String, t: String) = s + t }
    val bop2 = new BinaryOperator[Int]{ def apply(i: Int, j: Int) = i + j }
    val sbop = (s: String, t: String) => s + t
    val zbop = (i: Int, j: Int) => i + j
    def sameJ[A,B](f: BinaryOperator[A], g: BinaryOperator[B])(implicit ev1: A =:= B) =
      Box((a1: A, a2: A) => f.apply(a1, a2) == g.apply(ev1(a1), ev1(a2)))
    assert(sameJ(bop1, sbop.asJava).value(str,str))
    assert(sameJ(bop1, asJavaBinaryOperator(sbop)).value(str,str))
    // assert(sameJ(bop2, zbop.asJava).value(num, num))  -- IntBinaryOperator
    assert(sameJ(bop2, asJavaBinaryOperator(zbop)).value(num,num))
    assert(sameS(bop1.asScala, sbop).value(str,str))
    assert(sameS(asScalaFromBinaryOperator(bop1), sbop).value(str,str))
    assert(sameS(bop2.asScala, zbop).value(num,num))
    assert(sameS(asScalaFromBinaryOperator(bop2), zbop).value(num,num))
  }
  "test_BiPredicate" should "" in {
    val bip1 = new BiPredicate[String, File]{ def test(s: String, f: File) = s == f.getName }
    val bip2 = new BiPredicate[Int, Long]{ def test(i: Int, l: Long) = i == l }
    val sbip = (s: String, f: File) => s == f.getName
    val zbip = (i: Int, l: Long) => i == l
    def sameJ[A,B,C,D](f: BiPredicate[A,B], g: BiPredicate[C,D])(implicit ev1: A =:= C, ev2: B =:= D) =
      Box((a: A, b: B) => f.test(a,b) == g.test(ev1(a), ev2(b)))
    assert(sameJ(bip1, sbip.asJava).value(str,fyl))
    assert(sameJ(bip1, asJavaBiPredicate(sbip)).value(str,fyl))
    assert(sameJ(bip2, zbip.asJava).value(num,nmm))
    assert(sameJ(bip2, asJavaBiPredicate(zbip)).value(num, nmm))
    assert(sameS(bip1.asScala, sbip).value(str,fyl))
    assert(sameS(asScalaFromBiPredicate(bip1), sbip).value(str,fyl))
    assert(sameS(bip2.asScala, zbip).value(num, nmm))
    assert(sameS(asScalaFromBiPredicate(bip2), zbip).value(num,nmm))
  }
}