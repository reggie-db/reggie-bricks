import java.net.URI

pluginManagement {
  repositories {
    maven { url = java.net.URI("https://jitpack.io") }
    gradlePluginPortal()
  }
}

plugins {
  id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
  id("com.github.regbo.lfp-build") version "11d0abf64e"

}

rootProject.name = rootDir.name

dependencyResolutionManagement {
  repositories.add(
      repositories.maven {
        name = "Vaadin Directory"
        url = URI.create("https://maven.vaadin.com/vaadin-addons")
      })
}

