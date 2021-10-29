# Validators

A validator applies its conditions to a message. The message will be annotated with all the conditions that didn't match. Conditions are Boolean MongoDB expressions. These can be about the message as a whole or about specific fields. In the latter case the condition is only applied when the field exists. You can check the presence of a field with a specific condition that uses the `$exists` operator.

The result of a validation is the message itself. If there are validation errors the message will be annotated. The field `_error` will be set to `true` and the `errors` field will contain an array with all the errors. For each error, the array contains an object with the field `location`. This is a JSON pointer that refers to the location in the incoming message where the error occurs. The optional field `code` will contain the error code that is defined by the condition that failed.

Say we have the following validator, which checks that the field `myfield` is present and that it is an integer.

```yaml
---
conditions:
  - myfield:
      $exists: true
      $code: "REQUIRED"
    myfield:
      $type: "int"
      $code: "INT"    
```

The following message doesn't have the field `myfield`.

```json
{
  "field1": "value1",
  "field2": "value2"
}  
```

The validation then returns the following:

```json
{
  "field1": "value1",
  "field2": "value2",
  "_error": true,
  "errors": [
    {
      "location": "/",
      "code": "REQUIRED"      
    }    
  ]  
}  
```

The next message has the field, but it is a string instead of an integer.

```json
{
  "field1": "value1",
  "field2": "value2",
  "myfield": "0"  
}  
```

This will produce the following:

```json
{
  "field1": "value1",
  "field2": "value2",
  "_error": true,
  "errors": [
    {
      "location": "/myfield",
      "code": "INT"      
    }    
  ]  
}  
```

## Inclusions

A validator can include others with the `include` field, which expects an array of filenames. This makes validators more modular. The effect is that the conditions in the included validator are added to the including one. The combined conditions will be applied to the messages. Here is an example:

```yaml
---
include:
  - other.yml
conditions:
  - myfield:
      $exists: true
      $code: "REQUIRED"
```

other.yml:

```yaml
---
conditions:
  - myfield:
      $type: "int"
      $code: "INT"
```

The resulting validator will behave the same way as the first example of this chapter.

## Macros

Certain conditions occur in many places. Instead of rewriting them you can share them through macros. The field `macros` expects an object. Each key is the name of a macro. The value is the expression that is used as a condition. When you have a macro `mymacro`, then you can use it where an expression is expected by putting the string `_mymacro_` instead.

If an inclusion contains macros, then these are available in the including validator. However, if the latter defines some macro with a name that is also used in the included validator, then the including validator wins. You can include a validator with only macros and no conditions.

In the following example the integer condition is defined as a macro.

```yaml
---
macros:
  int:
    $type: "int"
    $code: "INT"
conditions:
  - field1: "_int_"
  - field2: "_int_"
```

## Nesting

Validators can be nested by creating a condition for a field with another validator as its value instead of a Boolean expression. You would apply this on subobjects. In that case the subobject is the context for the nested validator. In the following example there is a nested validator for the `_jwt` field.

```yaml
---
macros:
  exists:
    $exists: true
    $code: "REQUIRED"
conditions:
  - myfield: "_exists"
  - _jwt:
      conditions:
        - sub: "_exists_"
        - sub:
            $type: "string"
            $code: "STRING"
        - roles:
            $type: "array"
            $code: "ARRAY"        
```

In the following message the `_jwt.roles` field has the wrong type.

```json
{
  "myfield": "myvalue",
  "_jwt": {
    "sub": "memyselfandi",
    "roles": "myroles"    
  }
}  
```

The annotated message will be:

```json
{
  "myfield": "myvalue",
  "_jwt": {
    "sub": "memyselfandi",
    "roles": "myroles"    
  },
  "_error": true,
  "errors": [
    {
      "location": "/_jwt/roles",
      "code": "ARRAY"      
    }    
  ]  
}  
```

## References

Nesting of validators can be made more modular with the `ref` field, which expects a filename. The field will be replaced with the contents of the file. The referred validator doesn't have access to the macros in the referring validator.

The above nested validator can be rewritten like this:

```yaml
---
include:
  - "macros.yml"
conditions:
  - myfield: "_exists"
  - _jwt:
      ref: "jwt.yml"  
```

macros.yml:

```yaml
---
macros:
  exists:
    $exists: true
    $code: "REQUIRED"    
```

jwt.yml:

```yaml
---
include:
  - "macros.yml"
conditions:
  - sub: "_exists_"
  - sub:
      $type: "string"
      $code: "STRING"
  - roles:
      $type: "array"
      $code: "ARRAY"        
```