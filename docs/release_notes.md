# Release Notes

## 2.1

* Add the operators `$base64Encode`, `$base64Decode`, `$uriEncode`, `$uriDecode`, `$jsonToString` and `$stringToJson`.
* Add the command-line option `-d, --directory` to the commands `doc` and `dot`. This writes their outputs to the given directory using the application name to construct the filename in it.
* Make the command-line option `-a, --application` optional for the commands `doc` and `dot`. When no application is given all the deployed applications are run.
* Add the command-line option `-g, --global` to the `dot` command. It generates a graph that connects topics and applications for all the deployed applications.
* Make it possible to add prefixes and suffixes in parameter references.
* Fix leader and keep-alive exception.
* Fix aggregates using `dev` as the default environment.