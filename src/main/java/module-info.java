module application {
  requires net.pincette.common;
  requires com.schibsted.spt.data.jslt;
  requires net.pincette.json.streams.plugin;
  requires net.pincette.mongo;
  requires net.pincette.mongo.streams;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  requires info.picocli;
  requires org.mongodb.driver.core;
  requires java.logging;
  requires java.net.http;
  requires net.pincette.jes;
  requires net.pincette.jes.elastic;
  requires net.pincette.jes.util;
  requires net.pincette.rs;
  requires net.pincette.rs.streams;
  requires net.pincette.rs.kafka;
  requires net.pincette.kafka.json;
  requires kafka.clients;
  requires org.reactivestreams;
  requires typesafe.config;
  requires java.json;
  requires java.sql;
  requires net.pincette.json;
  requires jdk.unsupported;
  requires java.management;
  requires net.pincette.rs.json;
  requires software.amazon.awssdk.services.s3;
  requires software.amazon.awssdk.core;
  requires software.amazon.awssdk.services.secretsmanager;
  requires com.auth0.jwt;
  requires org.slf4j;

  uses net.pincette.json.streams.plugin.Plugin;
  uses javax.json.spi.JsonProvider;

  opens net.pincette.json.streams;

  exports net.pincette.json.streams;
}
