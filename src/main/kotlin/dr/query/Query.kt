package dr.query

import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.*

class DrQueryEngine() {
  fun query(query: String) {
    val lexer = QueryLexer(CharStreams.fromString(query))
    val tokens = CommonTokenStream(lexer)
    val parser = QueryParser(tokens)
    val tree = parser.prog()

    val walker = ParseTreeWalker()
    val listener = DrQueryListener()
    walker.walk(listener, tree)
  }
}

class DrQueryListener(): QueryBaseListener() {
  override fun enterProg(ctx: QueryParser.ProgContext) {
    val node = ctx.expr().forEach {
      println("${it.text}")
    }
  }

  override fun visitErrorNode(node: ErrorNode) {
    println("Error: $node")
  }
}