# The Command-line

Use `help`, `-h`, `command help` or `command -h` to get a short overview. The `-V` option shows the version of JSON Streams.

|Command|Option|Mandatory|Description|
|---|---|---|---|
|build|-f \| --file|Yes|A file with an array of applications. It will inline everything, resulting in one big JSON file. When no MongoDB collection has been specified it will dump the result on the terminal.|
||-c \| --collection|No|A MongoDB collection to which the generated file will be written using the application name as the ID. This means that each application in the given array will go to its own document. Existing documents are overwritten. When no collection is given the tool will try to take the MongoDB collection from the configuration. If the service is already running off the MongoDB collection, this command will start or restart the applications.|
||-l \| --local|No|Write the result to the terminal even when the configuration specifies a MongoDB collection. This option can't be used together with the collection option.|
|delete|-a \| --application|Yes|The name of the application that should be removed from the MongoDB collection. As a result, if the service is running off a MongoDB collection, the application will be stopped.|
||-c \| --collection|No|A MongoDB collection from which the application will be removed. When no collection is given the tool will try to take the MongoDB collection from the configuration.|
|doc|-f \| --file|No|A file with an application object for which documentation should be generated. This option can't be used with the next two. This writes a markdown file to `stdout`.|
||-a \| --application|No|The name of the application for which documentation should be generated off the MongoDB collection. Without this option all deployed applications are run.|
||-c \| --collection|No|A MongoDB collection from which the application is taken. When no collection is given the tool will try to take the MongoDB collection from the configuration.|
||-d \| --directory|No|Write the output to the given directory instead of to `stdout`. The application name will be used to construct the filename in the directory.|
||-e \| --exclude|No|Exclude applications from being processed.|
|dot|-f \| --file|No|A file with an application object for a dot file should be generated. This option can't be used with the next two.|
||-a \| --application|No|The name of the application for which a dot file should be generated off the MongoDB collection. Without this option all deployed applications are run.|
||-c \| --collection|No|A MongoDB collection from which the application is taken. When no collection is given the tool will try to take the MongoDB collection from the configuration.|
||-d \| --directory|No|Write the output to the given directory instead of to `stdout`. The application name will be used to construct the filename in the directory.|
||-g \| --global|No|Generate a graph of all the deployed applications. It will connect topics to applications.|
||-e \| --exclude|No|Exclude applications from being processed.|
|list|-c \| --collection|No|A MongoDB collection from which the application list is taken. When no collection is given the tool will try to take the MongoDB collection from the configuration.|
|run|-f \| --file|No|A file with an array of applications. It builds and then runs all the applications.|
||-c \| --collection|No|A MongoDB collection with applications. If neither a collection nor a file is given the tool will try to take the MongoDB collection from the configuration. This and the previous option can't be used together.|
||-q \| --query|No|A MongoDB query to select the applications to run.|
||-a \| --application|No|The name of the application that should be run off the MongoDB collection. This is a shorthand for the query `{"application": "<name>"}`.|
|test|-f \| --file|Yes|A file with an application object. It builds and then runs the application in test-mode.|
|yaml from|||Converts a YAML file on `stdin` to JSON and writes the result to `stdout`.|
|yaml to|||Converts a JSON file on `stdin` to YAML and writes the result to `stdout`.|