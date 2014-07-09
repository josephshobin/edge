//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

uniform.project("edge", "au.com.cba.omnia.edge")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  depend.scalaz() ++
  depend.shapeless() ++
  Seq(
    "com.twitter"      %% "scalding-avro"      % depend.versions.scalding
  )

uniformAssemblySettings

publishArtifact in Test := true

autoAPIMappings := true

apiMappings ++= {
  val cp: Seq[Attributed[File]] = (fullClasspath in Compile).value
  def assignApiUrl(organization: String, name: String, link: String): Option[(File, URL)] = {
    ( for {
      entry <- cp
      module <- entry.get(moduleID.key)
      if module.organization == organization
      if module.name.startsWith(name)
      jarFile = entry.data
    } yield jarFile
    ).headOption.map(_ -> url(link))
  }
  List(
    assignApiUrl("cascading", "cascading-core", "http://docs.cascading.org/cascading/2.5/javadoc"),
    assignApiUrl("cascading", "cascading-hadoop", "http://docs.cascading.org/cascading/2.5/javadoc"),
    assignApiUrl("cascading", "cascading-local", "http://docs.cascading.org/cascading/2.5/javadoc"),
    assignApiUrl("com.twitter", "scalding-core", "http://twitter.github.io/scalding/")
  ).flatten.toMap
}

site.settings ++ Seq(includeFilter in SiteKeys.makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.md" | "*.yml")



site.includeScaladoc()
