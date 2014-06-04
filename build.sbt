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
