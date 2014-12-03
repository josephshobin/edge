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
  depend.shapeless() ++ Seq(
    "au.com.cba.omnia" %% "permafrost" % "0.1.0-20141102230603-b90058b" % "test",
    "au.com.cba.omnia" %% "permafrost" % "0.1.0-20141102230603-b90058b" % "test" classifier "tests"
  )

updateOptions := updateOptions.value.withCachedResolution(true)

uniform.docSettings("https://github.com/CommBank/edge")

uniform.ghsettings
