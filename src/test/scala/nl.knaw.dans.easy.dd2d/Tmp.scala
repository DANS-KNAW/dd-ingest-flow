package nl.knaw.dans.easy.dd2d

class Tmp extends TestSupportFixture {
  "" should "" in {
    import java.io.File
    import java.util.function._
    import scala.compat.java8.FunctionConverters._

    val str = "fish"
    val fyl = new File("salmon")

    case class Box[A](value: A) {}


    //val xf = new Function[String, String=> File]{ def apply(s: String, f: File) = (s,f) }
    val bif = new BiFunction[String, File, (String, File)]{ def apply(s: String, f: File) = (s,f) }
    val sbif = (s: String, f: File) => (s,f)
    val jbif = sbif.asJava
    def sameJ[A,B,C,D,E,F](f: BiFunction[A, B, C], g: BiFunction[D, E, F])
                          (implicit ev1: A =:= D, ev2: B =:= E, ev3: C =:= F) = {
      Box((a: A, b: B) => {
        val c = f.apply(a, b)
        val e1 = ev1(a)
        val e2 = ev2(b)
        println(c)
        println(e1)
        println(e2)
        c == g.apply(e1, e2)
      }
      )
    }
    def x[A,B,C](f: BiFunction[A, B, C]) = {
      Box((a: A, b: B) => { f.apply(a, b) })
    }
    val xx = x(bif)
    val value1 = sameJ(bif, jbif)
    val bool = value1.value(str, fyl)
    assert(bool)
  }
}
