# Working with Qlerify (experimental)

[Qlerify](https://www.qlerify.com) is the best tool to do event-modeling. It matches well with 
JSON Streams. This chapter introduces a CLI-tool with which you can generate JSON Streams 
applications and the corresponding OpenAPI specifications.

You can install the tool through Homebrew like this:

```bash
brew tap wdonne/tap
brew install json-streams-qlerify
```

It comes with two subcommands. With the command `app` you generate an application and `api` 
produces the OpenAPI specifications for the REST and Server-Sent Events endpoints. Each of the 
commands requires the exported specification of a Qlerify event-model.

The main restriction is the way entities are handled. In general, entities can refer to other 
entities. There is no equivalent for this in JSON Streams. The only supported entity is the 
aggregate root. All entity references in the event-model are treated as value objects, which are 
always embedded in JSON Streams. That is because in JSON Streams, an aggregate is merely a JSON
document that is stored in MongoDB.

## Generating Applications

You can generate an app with the following command:

```bash
json-streams-qlerify --file <event-model.json> --directory <out>
```

It generates the file `<out>/application.yaml`, with the parts it refers to. The reducers are 
generated if they don't exist yet, because it is assumed you will change their contents manually.
The same goes for the command validators. The application also includes a file called 
`extra-parts.yaml`, which is also not overwritten.

With the extra option `--mock-mode`, the reducers and validators are always overwritten. The 
generated reducers simply merge the command fields into the aggregate instance. The validators 
will have the basic checks that are derived from the field definitions in Qlerify.

## Generating OpenAPI Specifications

The command for this is the following:

```bash
json-streams-qlerify --file <event-model.json> --directory <out> --template <template.yaml>
```

This generates the files `<out>/http.yaml` for the REST endpoint, and `<out>/sse.yaml` for the 
Server-Sent Events endpoint. The optional template file can be used to define fields in the top 
object of the specification. The generated fields will be merged into it.

You can create a stand-alone HTML page of a specification like this:

```bash
npx openapi-generate-html -i <out>/http.yaml -o http.html
```
