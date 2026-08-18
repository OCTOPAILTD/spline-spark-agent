/*
 * Copyright 2026 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.harvester.postprocessing.metadata

import fastparse.Parsed.{Failure, Success}
import fastparse.SingleLineWhitespace._
import fastparse._

import scala.reflect.runtime.universe._

/**
 * Evaluates a restricted method-chain expression against template bindings.
 * Replaces arbitrary JavaScript evaluation for metadata template rules.
 */
object SafePathEvaluator {

  private val AllowedZeroArgMethods = Set("name", "appName", "conf", "sparkContext")
  private val AllowedUnaryMethods = Set("get")

  def eval(expression: String, bindings: Map[String, Any]): AnyRef = {
    val steps = parse(expression)
    val root = bindings.getOrElse(
      steps.root,
      throw new IllegalArgumentException(s"Unknown binding root '${steps.root}' in safe path expression")
    )
    steps.methodCalls.foldLeft(root)((obj, call) => invokeCall(obj, call))
  }.asInstanceOf[AnyRef]

  private def parse(expression: String): ParsedExpression =
    fastparse.parse(expression.trim, parser(_)) match {
      case Success(value, _) => value
      case Failure(_, _, extra) => throw new IllegalArgumentException(extra.trace().longMsg)
    }

  private def invokeCall(obj: Any, call: MethodCall): Any = {
    if (call.arg.isEmpty) {
      if (!AllowedZeroArgMethods.contains(call.name)) {
        throw new IllegalArgumentException(s"Method '${call.name}' is not allowed in safe path expressions")
      }
      invokeZeroArg(obj, call.name)
    } else {
      if (!AllowedUnaryMethods.contains(call.name)) {
        throw new IllegalArgumentException(s"Method '${call.name}' is not allowed in safe path expressions")
      }
      invokeUnary(obj, call.name, call.arg.get)
    }
  }

  private def invokeZeroArg(obj: Any, methodName: String): Any = {
    val mirror = runtimeMirror(obj.getClass.getClassLoader)
    val instanceMirror = mirror.reflect(obj)
    val method = instanceMirror.symbol.typeSignature.member(TermName(methodName)).asMethod
    instanceMirror.reflectMethod(method).apply()
  }

  private def invokeUnary(obj: Any, methodName: String, arg: String): Any = {
    val mirror = runtimeMirror(obj.getClass.getClassLoader)
    val instanceMirror = mirror.reflect(obj)
    val method = instanceMirror.symbol.typeSignature.member(TermName(methodName)).asMethod
    instanceMirror.reflectMethod(method).apply(arg)
  }

  private case class ParsedExpression(root: String, methodCalls: Seq[MethodCall])
  private case class MethodCall(name: String, arg: Option[String])

  private def parser[_: P]: P[ParsedExpression] = P(root ~ methodCall.rep ~ End).map {
    case (root, calls) => ParsedExpression(root, calls)
  }

  private def root[_: P]: P[String] = P(CharIn("a-zA-Z@") ~ CharIn("a-zA-Z0-9").rep).!

  private def methodCall[_: P]: P[MethodCall] = P(
    "." ~/ methodName ~ "(" ~/ (stringLiteral.?).? ~ ")"
  ).map { case (name, arg) => MethodCall(name, arg) }

  private def methodName[_: P]: P[String] = P(CharIn("a-zA-Z") ~ CharIn("a-zA-Z0-9").rep).!

  private def stringLiteral[_: P]: P[String] = P(
    ("'" ~/ CharsWhile(_ != '\'', 0).! ~ "'") | ("\"" ~/ CharsWhile(_ != '"', 0).! ~ "\"")
  )
}
