package com.reggie.bricks.tools

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * Primary Spring Boot configuration for the CLI tooling package.
 * Defines component scanning boundaries and shared configuration beans.
 */
@SpringBootApplication
class App

/**
 * Bootstraps the tooling application.
 *
 * @param args command-line arguments forwarded to Spring Boot.
 */
fun main(args: Array<String>) {
    runApplication<App>(*args)
}
