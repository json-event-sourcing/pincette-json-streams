# Custom JSLT Functions

JSON Streams comes with a few custom functions you can use in your JSLT scripts.

## base64-decode

The function accepts one argument, which should be a Base64 string. The decoded bytes are interpreted as a string that is encoded in UTF-8.

## base64-encode

It takes its only argument, which should be a string, and performs Base64 encoding on it.

## get-pointer

This function has at least two arguments. The first one is a JSON object and the second a JSON pointer, which is used to extract a value from the object. The optional third argument is a default value in case the value was not found. Without a third argument the default result will be `null`.

## parse-iso-instant

It has one argument, which is interpreted as an ISO 8601 timestamp. It returns an epoch seconds value.

## pointer

The function has at least one argument, but it can have any number above that. All arguments are combined to create a JSON pointer, which can be used in the `get-pointer` function.

## set-pointer

This function requires exactly three arguments. The first one is a JSON object. The second one is a JSON pointer and the last one is the value that is set in the object at the given location. The result is a new JSON object.

## substr

This function requires two or three arguments: a string, a start index and optionally an end index. The default of the latter is the length of the string. The function returns the substring of the given string.

## trace

It requires one argument, which is returned unmodified. As a side-effect the argument is traced in the Java logger `pincette-json-streams`.

## uri-decode

It performs URI-decoding on its only mandatory argument.

## uri-encode

It performs URI-encoding on its only mandatory argument.

## uuid

This function has no arguments. It generates a UUID.
