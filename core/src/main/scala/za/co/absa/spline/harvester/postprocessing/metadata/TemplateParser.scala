/*
 * Copyright 2022 ABSA Group Limited
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

object TemplateParser {

  def parse(templates: Map[String, Any], securityContext: TemplateSecurityContext = TemplateSecurityContext()): DataTemplate = {
    val extraTemplate = getTemplate(templates, Key.Extra)
    val labelsTemplate = getTemplate(templates, Key.Labels)
    assert(extraTemplate.nonEmpty || labelsTemplate.nonEmpty)

    new DataTemplate(
      parseTemplate(extraTemplate, securityContext),
      parseTemplate(labelsTemplate, securityContext),
      securityContext
    )
  }

  private def getTemplate(template: Map[String, Any], key: String) =
    template.get(key).map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty)

  private def parseTemplate(template: Map[String, Any], securityContext: TemplateSecurityContext) =
    template.transform((_, v) => parseRec(v, securityContext))

  private def parseRec(v: Any, securityContext: TemplateSecurityContext): Any = v match {
    case m: Map[_, _] => m.toSeq match {
      case Seq((EvaluableNames.JVMProp, v: String)) => JVMProp(v)
      case Seq((EvaluableNames.EnvVar, v: String)) => EnvVar(v)
      case Seq((EvaluableNames.JsEval, v: String)) => SafePathEval(v)
      case s: Seq[(_, _)] =>
        assert(!s.exists(_._1.toString.startsWith("$")))
        s.map { case (k, v) => k -> parseRec(v, securityContext) }.toMap
    }
    case s: Seq[_] => s.map(parseRec(_, securityContext))
    case v => v
  }

  object Key {
    val Extra = "extra"
    val Labels = "labels"
  }
}
