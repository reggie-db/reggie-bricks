plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.vaadin)
    application
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    implementation("com.vaadin:vaadin-spring-boot-starter:${libs.versions.vaadin.get()}")
    implementation("org.springframework.boot:spring-boot-starter-validation")
//    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation(libs.databricks)
//    implementation("com.openai:openai-java:3.0.3")
//
//    implementation("com.databricks:databricks-connect:16.4.6")
//
//    implementation("org.scala-lang:scala-library:2.12.18")
//    implementation("org.scala-lang:scala-reflect:2.12.18")
//    runtimeOnly("com.databricks:databricks-jdbc:2.7.3")
//    runtimeOnly("org.postgresql:postgresql")

//    implementation("org.dflib:dflib-jdbc:1.3.0")

    //    testImplementation("org.apache.spark:spark-sql_2.13:3.5.2"){
    //        exclude(group = "org.apache.logging.log4j")
    //        exclude(group = "org.slf4j")
    //        exclude(group = "ch.qos.logback")
    //    }
    //    testImplementation("org.apache.spark:spark-core_2.13:3.5.2"){
    //        exclude(group = "org.apache.logging.log4j")
    //        exclude(group = "org.slf4j")
    //        exclude(group = "ch.qos.logback")
    //    }
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    )
}

application {
    // Define the main class for the application.
    mainClass = "document.hub.app.AppKt"
    applicationDefaultJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
