package net.pincette.json.streams;

import static net.pincette.rs.Mapper.map;

import net.pincette.mongo.streams.Stage;
import net.pincette.util.Util.GeneralException;

class TestExceptionStage {
  private TestExceptionStage() {}

  static Stage testExceptionStage() {
    return (expression, c) -> map(m -> {
      throw new GeneralException("testException");
    });
  }
}
