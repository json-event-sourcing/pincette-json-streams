# Serialisation

JSON Streams only exchanges JSON messages. They are serialised in [compressed](https://datatracker.ietf.org/doc/html/rfc1952) [CBOR](https://tools.ietf.org/html/rfc7049). The deserialiser expects messages to be in this format, but it will fall back to a string serialisation when that fails. Messages can also be produced as strings by setting the `toString` field in stream parts to `true`.
