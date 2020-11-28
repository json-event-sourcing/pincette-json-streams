module application {
  requires net.pincette.common;
  requires jslt;
  requires net.pincette.json.streams.plugin;
  requires net.pincette.mongo;
  requires net.pincette.mongo.streams;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  requires info.picocli;
  requires org.mongodb.driver.core;
  requires java.logging;
  requires kafka.streams;
  requires net.pincette.jes;
  requires net.pincette.jes.elastic;
  requires net.pincette.jes.util;
  requires net.pincette.rs;
  requires kafka.clients;
  requires org.reactivestreams;
  requires typesafe.config;
  requires java.json;
  requires net.pincette.json;
  requires jdk.unsupported;

  uses net.pincette.json.streams.plugin.Plugin;
  uses javax.json.spi.JsonProvider;

  opens net.pincette.json.streams to
      info.picocli;
}
