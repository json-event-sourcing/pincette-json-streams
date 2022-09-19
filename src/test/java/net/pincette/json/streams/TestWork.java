package net.pincette.json.streams;

import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.streams.Work.simulate;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.json.JsonReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestWork {
  private static void runTest(final String name) {
    tryToGetWithRethrow(
            () -> createReader(TestWork.class.getResourceAsStream("/" + name + ".json")),
            JsonReader::readObject)
        .ifPresent(json -> assertEquals(json.getInt("expected"), simulate(json)));
  }

  @Test
  @DisplayName("work1")
  void work1() {
    runTest("work1");
  }

  @Test
  @DisplayName("work2")
  void work2() {
    runTest("work2");
  }

  @Test
  @DisplayName("work3")
  void work3() {
    runTest("work3");
  }

  @Test
  @DisplayName("work4")
  void work4() {
    runTest("work4");
  }

  @Test
  @DisplayName("work5")
  void work5() {
    runTest("work5");
  }

  @Test
  @DisplayName("work6")
  void work6() {
    runTest("work6");
  }

  @Test
  @DisplayName("work7")
  void work7() {
    runTest("work7");
  }

  @Test
  @DisplayName("work8")
  void work8() {
    runTest("work8");
  }

  @Test
  @DisplayName("work9")
  void work9() {
    runTest("work9");
  }

  @Test
  @DisplayName("work10")
  void work10() {
    runTest("work10");
  }

  @Test
  @DisplayName("work11")
  void work11() {
    runTest("work11");
  }

  @Test
  @DisplayName("work12")
  void work12() {
    runTest("work12");
  }

  @Test
  @DisplayName("work13")
  void work13() {
    runTest("work13");
  }

  @Test
  @DisplayName("work14")
  void work14() {
    runTest("work14");
  }
}
