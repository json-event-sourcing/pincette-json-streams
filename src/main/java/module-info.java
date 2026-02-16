module application {
  requires net.pincette.json.streams.plugin;
  requires net.pincette.mongo;
  requires net.pincette.mongo.streams;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  requires info.picocli;
  requires org.mongodb.driver.core;
  requires java.net.http;
  requires net.pincette.jes;
  requires net.pincette.jes.util;
  requires net.pincette.rs;
  requires net.pincette.rs.streams;
  requires net.pincette.rs.kafka;
  requires net.pincette.kafka.json;
  requires kafka.clients;
  requires org.reactivestreams;
  requires java.sql;
  requires jdk.unsupported;
  requires java.management;
  requires net.pincette.rs.json;
  requires software.amazon.awssdk.services.s3;
  requires software.amazon.awssdk.core;
  requires com.auth0.jwt;
  requires org.slf4j;
  requires io.opentelemetry.sdk.logs;
  requires io.opentelemetry.exporter.otlp;
  requires net.pincette.config;
  requires typesafe.config;
  requires io.opentelemetry.sdk.common;
  requires net.pincette.jes.tel;
  requires io.opentelemetry.api;
  requires net.pincette.json;
  requires net.pincette.common;
  requires java.json;
  requires io.opentelemetry.sdk;
  requires io.opentelemetry.sdk.metrics;
  requires com.schibsted.spt.data.jslt;
  requires org.mongodb.driver.sync.client;
  requires net.pincette.s3util;
  requires transitive org.jruby.joni;
  requires com.fasterxml.jackson.databind;

  uses net.pincette.json.streams.plugin.Plugin;
  uses javax.json.spi.JsonProvider;

  opens net.pincette.json.streams;

  exports net.pincette.json.streams;
}
