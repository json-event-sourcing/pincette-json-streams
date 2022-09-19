package net.pincette.json.streams;

import static net.pincette.jes.JsonFields.ERROR;
import static net.pincette.jes.JsonFields.ERRORS;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.streams.Common.VALIDATE;
import static net.pincette.json.streams.Logging.logStageObject;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.PassThrough.passThrough;

import java.util.Optional;
import net.pincette.mongo.streams.Stage;

class ValidateStage {
  private ValidateStage() {}

  static Stage validateStage(final Context context) {
    return (expression, c) -> {
      if (!isObject(expression)) {
        logStageObject(VALIDATE, expression);

        return passThrough();
      }

      final var validator = context.validator.validator(expression.asJsonObject());

      return map(
          m ->
              m.withValue(
                  Optional.of(validator.apply(m.value))
                      .filter(errors -> !errors.isEmpty())
                      .map(
                          errors ->
                              createObjectBuilder(m.value)
                                  .add(ERROR, true)
                                  .add(ERRORS, errors)
                                  .build())
                      .orElse(m.value)));
    };
  }
}
