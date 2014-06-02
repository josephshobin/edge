uniform.project("edge", "au.com.cba.omnia.edge")

uniformDependencySettings

libraryDependencies :=
  depend.scaldingproject() ++
  depend.scalaz() ++
  Seq(
    "com.twitter"      %% "scalding-avro"      % depend.versions.scalding,
    "com.chuusai"      %% "shapeless"          % "2.0.0-M1" cross CrossVersion.full
  )

uniformAssemblySettings

publishArtifact in Test := true
