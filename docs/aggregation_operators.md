# Aggregation Pipeline Operators

This page lists the supported [MongoDB aggregation pipeline operators](https://docs.mongodb.com/manual/reference/operator/aggregation/), with possible deviations.

## Arithmetic

### [$abs](https://docs.mongodb.com/manual/reference/operator/aggregation/abs/#mongodb-expression-exp.-abs)

Example:

```yaml
$abs: 1
```

### [$add](https://docs.mongodb.com/manual/reference/operator/aggregation/add/#mongodb-expression-exp.-add)

Example:

```yaml
$add:
- "$field1"
- "$field2"
- "$field3"
```

### [$ceil](https://docs.mongodb.com/manual/reference/operator/aggregation/ceil/#mongodb-expression-exp.-ceil)

Example:

```yaml
$ceil: 1
```

### [$divide](https://docs.mongodb.com/manual/reference/operator/aggregation/divide/#mongodb-expression-exp.-divide)

Example:

```yaml
$divide:
- "$hours"
- 8
```

### [$exp](https://docs.mongodb.com/manual/reference/operator/aggregation/exp/#mongodb-expression-exp.-exp)

Example:

```yaml
$exp: 2
```

### [$floor](https://docs.mongodb.com/manual/reference/operator/aggregation/floor/#mongodb-expression-exp.-floor)

Example:

```yaml
$floor: 1
```

### [$ln](https://docs.mongodb.com/manual/reference/operator/aggregation/ln/#mongodb-expression-exp.-ln)

Example:

```yaml
$ln: 1
```

### [$log](https://docs.mongodb.com/manual/reference/operator/aggregation/log/#mongodb-expression-exp.-log)

Example:

```yaml
$log:
- 100
- 10
```

### [$log10](https://docs.mongodb.com/manual/reference/operator/aggregation/log10/#mongodb-expression-exp.-log10)

Example:

```yaml
$log10: 1
```

### [$mod](https://docs.mongodb.com/manual/reference/operator/aggregation/mod/#mongodb-expression-exp.-mod)

Example:

```yaml
$mod:
- "$myfield"
- 4
```

### [$multiply](https://docs.mongodb.com/manual/reference/operator/aggregation/multiply/#mongodb-expression-exp.-multiply)

Example:

```yaml
$multiply:
- "$field1"
- "$field2"
- "$field3"
```

### [$pow](https://docs.mongodb.com/manual/reference/operator/aggregation/pow/#mongodb-expression-exp.-pow)

Example:

```yaml
$pow:
- "$myfield"
- 2
```

### [$round](https://docs.mongodb.com/manual/reference/operator/aggregation/round/#mongodb-expression-exp.-round)

Example:

```yaml
$round:
- "$value"
- 0
```

### [$sqrt](https://docs.mongodb.com/manual/reference/operator/aggregation/sqrt/#mongodb-expression-exp.-sqrt)

Example:

```yaml
$sqrt: 25
```

### [$subtract](https://docs.mongodb.com/manual/reference/operator/aggregation/subtract/#mongodb-expression-exp.-subtract)

Example:

```yaml
$subtract:
- "$field1"
- "$field2"
```

### [$trunc](https://docs.mongodb.com/manual/reference/operator/aggregation/trunc/#mongodb-expression-exp.-trunc)

Example:

```yaml
$trunc:
- "$myfield"
- 0
```

## Array

### [$arrayElemAt](https://docs.mongodb.com/manual/reference/operator/aggregation/trunc/#mongodb-expression-exp.-trunc)

Example:

```yaml
$arrayElemAt:
-
  - 0
  - 1
  - 3
- 1
```

### [$arrayToObject](https://docs.mongodb.com/manual/reference/operator/aggregation/arrayToObject/#mongodb-expression-exp.-arrayToObject)

Example:

```yaml
$arrayToObject:
-
  - "field1"
  - 23
-
  - "field2"
  - 36
```

### [$concatArrays](https://docs.mongodb.com/manual/reference/operator/aggregation/concatArrays/#mongodb-expression-exp.-concatArrays)

Example:

```yaml
$concatArrays:
- "$array1"
- "$array2"
- "$array3"
```

### $elemMatch

This is actually a projection operator, but it just works on arrays and in a streaming setting it is more useful as an aggregation operator. The operator returns the first element of a given array that meets the condition.

Example:

```yaml
$elemMatch:
- "$myarray"
- $gt:
    1
```

### [$filter](https://docs.mongodb.com/manual/reference/operator/aggregation/filter/#mongodb-expression-exp.-filter)

Example:

```yaml
$filter:
  input: "$items"
  as: "item"
  cond:
    $gte:
    - "$$item.price"
    - 100
```

### [$first](https://docs.mongodb.com/manual/reference/operator/aggregation/first-array-element/#mongodb-expression-exp.-first)

Example:

```yaml
$first: "$myarray"
```

### [$in](https://docs.mongodb.com/manual/reference/operator/aggregation/in/#mongodb-expression-exp.-in)

Example:

```yaml
$in:
- 2
- "$myarray"
```

### [$indexOfArray](https://docs.mongodb.com/manual/reference/operator/aggregation/indexOfArray/#mongodb-expression-exp.-indexOfArray)

Example:

```yaml
$indexOfArray:
- "$myarray"
- 3
- 0
- 10
```

### [$isArray](https://docs.mongodb.com/manual/reference/operator/aggregation/isArray/#mongodb-expression-exp.-isArray)

Example:

```yaml
$isArray: "$myfield"
```

### [$last](https://docs.mongodb.com/manual/reference/operator/aggregation/last-array-element/#mongodb-expression-exp.-last)

Example:

```yaml
$last: "$myarray"
```

### [$map](https://docs.mongodb.com/manual/reference/operator/aggregation/map/#mongodb-expression-exp.-map)

Example:

```yaml
$map:
  input: "$myarray"
  as: "el"
  in:
    $add:
    - "$$el"
    - 1
```

### [$objectToArray](https://docs.mongodb.com/manual/reference/operator/aggregation/objectToArray/#mongodb-expression-exp.-objectToArray)

Example:

```yaml
$objectToArray
  field1: 1
  field2: 2
```

### [$range](https://docs.mongodb.com/manual/reference/operator/aggregation/range/#mongodb-expression-exp.-range)

Example:

```yaml
$range:
- 0
- 10
- 2
```

### [$reduce](https://docs.mongodb.com/manual/reference/operator/aggregation/reduce/#mongodb-expression-exp.-reduce)

Example:

```yaml
$reduce:
  input: "$myarray"
  initialValue: ""
  in:
    $concat:
    - "$$value"
    - "$$this"
```

### [$reverseArray](https://docs.mongodb.com/manual/reference/operator/aggregation/reverseArray/#mongodb-expression-exp.-reverseArray)

Example:

```yaml
$reverseArray: "$myarray"
```

### [$size](https://docs.mongodb.com/manual/reference/operator/aggregation/size/#mongodb-expression-exp.-size)

Example:

```yaml
$size: "$myarray"
```

### [$slice](https://docs.mongodb.com/manual/reference/operator/aggregation/slice/#mongodb-expression-exp.-slice)

Example:

```yaml
$slice:
- "$myarray",
- 1
- 2
```

### [$zip](https://docs.mongodb.com/manual/reference/operator/aggregation/zip/#mongodb-expression-exp.-zip)

Example:

```yaml
$zip:
- "$array1"
- "$array2"
```

## Boolean

### [$and](https://docs.mongodb.com/manual/reference/operator/aggregation/and/#mongodb-expression-exp.-and)

Example:

```yaml
$and:
- $gt:
  - "$myfield"
  - 0
- $lt:
  - "$myfield"
  - 10
```

### [$not](https://docs.mongodb.com/manual/reference/operator/aggregation/not/#mongodb-expression-exp.-not)

Example:

```yaml
$not:
- $gt:
  - "$myfield"
  - 0
```

### [$or](https://docs.mongodb.com/manual/reference/operator/aggregation/or/#mongodb-expression-exp.-or)

Example:

```yaml
$or:
- $lt:
  - "$myfield"
  - 10
- $gt:
  - "$myfield"
  - 20
```

## Comparison

### [$cmp](https://docs.mongodb.com/manual/reference/operator/aggregation/cmp/#mongodb-expression-exp.-cmp)

Example:

```yaml
$cmp:
- "$myfield"
- 100
```

### [$eq](https://docs.mongodb.com/manual/reference/operator/aggregation/eq/#mongodb-expression-exp.-eq)

Example:

```yaml
$eq:
- "$myfield"
- 100
```

### [$gt](https://docs.mongodb.com/manual/reference/operator/aggregation/gt/#mongodb-expression-exp.-gt)

Example:

```yaml
$gt:
- "$myfield"
- 100
```

### [$gte](https://docs.mongodb.com/manual/reference/operator/aggregation/gte/#mongodb-expression-exp.-gte)

Example:

```yaml
$gte:
- "$myfield"
- 100
```

### [$lt](https://docs.mongodb.com/manual/reference/operator/aggregation/lt/#mongodb-expression-exp.-lt)

Example:

```yaml
$lt:
- "$myfield"
- 100
```

### [$lte](https://docs.mongodb.com/manual/reference/operator/aggregation/lte/#mongodb-expression-exp.-lte)

Example:

```yaml
$lte:
- "$myfield"
- 100
```

### [$ne](https://docs.mongodb.com/manual/reference/operator/aggregation/ne/#mongodb-expression-exp.-ne)

Example:

```yaml
$ee:
- "$myfield"
- 100
```

## Conditional

### [$cond](https://docs.mongodb.com/manual/reference/operator/aggregation/cond/#mongodb-expression-exp.-cond)

Example:

```yaml
$cond:
  if:
    $gt:
    - "$myfield"
    - 10
  then: 1
  else: 0
```

### [$ifNull](https://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/#mongodb-expression-exp.-ifNull)

This is the MongoDB 4.4 variant.

Example:

```yaml
$ifNull:
- "$myfield"
- "My default value"
```

### [$switch](https://docs.mongodb.com/manual/reference/operator/aggregation/switch/#mongodb-expression-exp.-switch)

Example:

```yaml
$switch:
  branches:
  - case:
      $gt:
      - "$myfield"
      - 10
    then: "large"
  - case:
      $gt:
      - "$myfield"
      - 0
    then: "small"
  default: "Unknown"
```

## Literal

### [$literal](https://docs.mongodb.com/manual/reference/operator/aggregation/literal/#mongodb-expression-exp.-literal)

Example:

```yaml
$literal:
  $add:
    - 2
    - 3
```

## Object

### [$mergeObjects](https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/#mongodb-expression-exp.-mergeObjects)

Example:

```yaml
$mergeObjects:
- "$object1"
- "$object2"
- "$object3"
```

### [$objectToArray](https://docs.mongodb.com/manual/reference/operator/aggregation/objectToArray/#mongodb-expression-exp.-objectToArray)

See [$objectToArray](#objecttoarray).

## Set

### [$allElementsTrue](https://docs.mongodb.com/manual/reference/operator/aggregation/allElementsTrue/#mongodb-expression-exp.-allElementsTrue)

Example:

```yaml
$allElementsTrue:
- "$field1"
- $eq:
  - "$field2"
  - 1
```

### [$anyElementTrue](https://docs.mongodb.com/manual/reference/operator/aggregation/anyElementTrue/#mongodb-expression-exp.-anyElementTrue)

Example:

```yaml
$anyElementTrue:
- "$field1"
- $eq:
  - "$field2"
  - 1
```

### [$setDifference](https://docs.mongodb.com/manual/reference/operator/aggregation/setDifference/#mongodb-expression-exp.-setDifference)

Example:

```yaml
$setDifference:
- "$array1"
- "$array2"
```

### [$setEquals](https://docs.mongodb.com/manual/reference/operator/aggregation/setEquals/#mongodb-expression-exp.-setEquals)

Example:

```yaml
$setEquals:
- "$array1"
- "$array2"
```

### [$setIntersection](https://docs.mongodb.com/manual/reference/operator/aggregation/setIntersection/#mongodb-expression-exp.-setIntersection)

Example:

```yaml
$setIntersection:
- "$array1"
- "$array2"
- "$array3"
```

### [$setIsSubset](https://docs.mongodb.com/manual/reference/operator/aggregation/setIsSubset/#mongodb-expression-exp.-setIsSubset)

Example:

```yaml
$setIsSubset:
- "$array1"
- "$array2"
```

### [$setUnion](https://docs.mongodb.com/manual/reference/operator/aggregation/setUnion/#mongodb-expression-exp.-setUnion)

Example:

```yaml
$setUnion:
- "$array1"
- "$array2"
- "$array3"
```

## String

### [$concat](https://docs.mongodb.com/manual/reference/operator/aggregation/concat/#mongodb-expression-exp.-concat)

Example:

```yaml
$concat:
- "$field1"
- "$field2"
- "$field3"
```

### [$indexOfCP](https://docs.mongodb.com/manual/reference/operator/aggregation/indexOfCP/#mongodb-expression-exp.-indexOfCP)

Example:

```yaml
$indexOfCP:
- "abcd"
- "bc"
- 0
- 3
```

### [$ltrim](https://docs.mongodb.com/manual/reference/operator/aggregation/ltrim/#mongodb-expression-exp.-ltrim)

Example:

```yaml
$ltrim:
  input: "$myfield"
```

### [$regexFind](https://docs.mongodb.com/manual/reference/operator/aggregation/regexFind/#mongodb-expression-exp.-regexFind)

Example:

```yaml
$regexFind:
  input: "$myfield"
  regex: "/^test/"
```

### [$regexFindAll](https://docs.mongodb.com/manual/reference/operator/aggregation/regexFindAll/#mongodb-expression-exp.-regexFindAll)

Example:

```yaml
$regexFindAll:
  input: "$myfield"
  regex: "/^test/"
```

### [$regexMatch](https://docs.mongodb.com/manual/reference/operator/aggregation/regexMatch/#mongodb-expression-exp.-regexMatch)

Example:

```yaml
$regexMatch:
  input: "$myfield"
  regex: "/^test/"
```

### [$rtrim](https://docs.mongodb.com/manual/reference/operator/aggregation/rtrim/#mongodb-expression-exp.-rtrim)

Example:

```yaml
$rtrim:
  input: "$myfield"
```

### [$split](https://docs.mongodb.com/manual/reference/operator/aggregation/split/#mongodb-expression-exp.-split)

Example:

```yaml
$split:
- "$myfield"
- ","
```

### [$strLenCP](https://docs.mongodb.com/manual/reference/operator/aggregation/strLenCP/#mongodb-expression-exp.-strLenCP)

Example:

```yaml
$strLenCP: "$myfield"
```

### [$strcasecmp](https://docs.mongodb.com/manual/reference/operator/aggregation/strcasecmp/#mongodb-expression-exp.-strcasecmp)

Example:

```yaml
$strcasecmp:
- "$field1"
- "$field2"
```

### [$substrCP](https://docs.mongodb.com/manual/reference/operator/aggregation/substrCP/#mongodb-expression-exp.-substrCP)

Example:

```yaml
$substrCP:
- "abcd"
- 1
- 2
```

### [$toLower](https://docs.mongodb.com/manual/reference/operator/aggregation/toLower/#mongodb-expression-exp.-toLower)

Example:

```yaml
$toLower: "$myfield"
```

### [$toString](https://docs.mongodb.com/manual/reference/operator/aggregation/toString/#mongodb-expression-exp.-toString)

Example:

```yaml
$toString: 123
```

### [$toUpper](https://docs.mongodb.com/manual/reference/operator/aggregation/toUpper/#mongodb-expression-exp.-toUpper)

Example:

```yaml
$toUpper: "$myfield"
```

### [$trim](https://docs.mongodb.com/manual/reference/operator/aggregation/trim/#mongodb-expression-exp.-trim)

Example:

```yaml
$trim:
  input: "$myfield"
```

## Trigonometry

### [$acos](https://docs.mongodb.com/manual/reference/operator/aggregation/acos/#mongodb-expression-exp.-acos)

Example:

```yaml
$acos:
  $divide:
  - "$side_b"
  - "$hypotenuse"
```

### [$acosh](https://docs.mongodb.com/manual/reference/operator/aggregation/acosh/#mongodb-expression-exp.-acosh)

Example:

```yaml
$acosh : "$x-coordinate"
```

### [$asin](https://docs.mongodb.com/manual/reference/operator/aggregation/asin/#mongodb-expression-exp.-asin)

Example:

```yaml
$asin:
  $divide:
  - "$side_b"
  - "$hypotenuse"
```

### [$asinh](https://docs.mongodb.com/manual/reference/operator/aggregation/asinh/#mongodb-expression-exp.-asinh)

Example:

```yaml
$asinh : "$x-coordinate"
```

### [$atan](https://docs.mongodb.com/manual/reference/operator/aggregation/atan/#mongodb-expression-exp.-atan)

Example:

```yaml
$atan:
  $divide:
  - "$side_b"
  - "$side_a"
```

### [$atan2](https://docs.mongodb.com/manual/reference/operator/aggregation/atan2/#mongodb-expression-exp.-atan2)

Example:

```yaml
$atan2:
  $divide:
  - "$side_b"
  - "$side_a"
```

### [$atanh](https://docs.mongodb.com/manual/reference/operator/aggregation/atanh/#mongodb-expression-exp.-atanh)

Example:

```yaml
$atanh : "$x-coordinate"
```

### [$cos](https://docs.mongodb.com/manual/reference/operator/aggregation/cos/#mongodb-expression-exp.-cos)

Example:

```yaml
$cos:
  $degreesToRadians: "$angla_a:"
```

### [$cosh](https://docs.mongodb.com/manual/reference/operator/aggregation/cosh/#mongodb-expression-exp.-cosh)

Example:

```yaml
$cosh:
  $degreesToRadians: "$angle"
```

### [$degreesToRadians](https://docs.mongodb.com/manual/reference/operator/aggregation/degreesToRadians/#mongodb-expression-exp.-degreesToRadians)

Example:

```yaml
$degreesToRadians: "$angle"
```


### [$radiansToDegrees](https://docs.mongodb.com/manual/reference/operator/aggregation/radiansToDegrees/#mongodb-expression-exp.-radiansToDegrees)

Example:

```yaml
$radiansToDegrees: "$angle"
```

### [$sin](https://docs.mongodb.com/manual/reference/operator/aggregation/sin/#mongodb-expression-exp.-sin)

Example:

```yaml
$sin:
  $degreesToRadians: "$angle"
```

### [$sinh](https://docs.mongodb.com/manual/reference/operator/aggregation/sinh/#mongodb-expression-exp.-sinh)

Example:

```yaml
$sinh:
  $degreesToRadians: "$angle"
```

### [$tan](https://docs.mongodb.com/manual/reference/operator/aggregation/tan/#mongodb-expression-exp.-tan)

Example:

```yaml
$tan:
  $degreesToRadians: "$angle"
```

### [$tanh](https://docs.mongodb.com/manual/reference/operator/aggregation/tanh/#mongodb-expression-exp.-tanh)

Example:

```yaml
$tanh:
  $degreesToRadians: "$angle"
```

## Type

### [$convert](https://docs.mongodb.com/manual/reference/operator/aggregation/convert/#mongodb-expression-exp.-convert)

Example:

```yaml
$convert:
  input: "hello"
  to: "bool"
```

### [$toBool](https://docs.mongodb.com/manual/reference/operator/aggregation/toBool/#mongodb-expression-exp.-toBool)

Example:

```yaml
$toBool: "hello"
```

### [$toDecimal](https://docs.mongodb.com/manual/reference/operator/aggregation/toDecimal/#mongodb-expression-exp.-toDecimal)

Example:

```yaml
$toDecimal: "0.1"
```

### [$toDouble](https://docs.mongodb.com/manual/reference/operator/aggregation/toDouble/#mongodb-expression-exp.-toDouble)

Example:


```yaml
$toDouble: "0.1"
```

### [$toInt](https://docs.mongodb.com/manual/reference/operator/aggregation/toInt/#mongodb-expression-exp.-toInt)

Example:

```yaml
$toInt: "5"
```

### [$toLong](https://docs.mongodb.com/manual/reference/operator/aggregation/toLong/#mongodb-expression-exp.-toLong)

Example:

```yaml
$toLong: "5"
```

### [$toString](https://docs.mongodb.com/manual/reference/operator/aggregation/toString/#mongodb-expression-exp.-toString)

Example:

```yaml
$toString: 100
```

### [$type](https://docs.mongodb.com/manual/reference/operator/aggregation/type/#mongodb-expression-exp.-type)

Example:

```yaml
$type: "$myfield"
```

## Miscellaneous

### [$let](https://docs.mongodb.com/manual/reference/operator/aggregation/let/)

Example:

```yaml
$let:
  vars:
    low: 1
    high: "$$low"
  in:
    $gt:
    - "$$low"
    - "$$high"
```    

### $sort

The `$sort` extension operator receives an object with the mandatory field `input`, which should be an expression that yields an array. The optional field `direction` can have the values `asc` or `desc`, the former being the default. The optional field `paths` is a list of field paths. When it is present, only object values in the array are considered. They will be sorted hierarchically with the values extracted with the paths. So, if the values from the first path are equal, then the next path is tried and so on.

Example:

```yaml
$sort:
  input: "$myarray"
  direction: "desc"
  paths:
  - "a.b.c"
  - "a.d.e"
  - "a.f"
```  
 
### $trace

You can wrap any expression with the `$trace` extension operator. This doesn't alter the result of the expression. When the log level of the Java logger `net.pincette.mongo.expressions` is set to `INFO`, the wrapped expression will be traced. If the level is `FINEST` all expressions are traced, regardless of the presence of the `$trace` operator.

Example:

```yaml
$trace:
  $toLower: "HELLO"
```

## State Management

There are a few extension operators to help with state management, which is implemented with [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes).

### $jes-added

The expression of this operator is an object with the field pointer. It should be an expression that yields a JSON pointer. The operator returns true if the pointed field was added in the event.

Example:

```yaml
type: "stream"
name: "myapp-mytype-added"
fromStream: "myapp-mytype-event"
pipeline:
- $match:
    $expr:
      $jes-added: "/myfield"    
```

### $jes-changed

The expression of this operator is an object with the fields `pointer`, `from` and `to`. Only the first field is mandatory. It should be an expression that yields a JSON pointer. The operator returns true if the pointed field has changed in the event. If both the `from` and `to` fields are present then the operator returns true when the pointed field in the event has transitioned between the two values produced by their expressions. In that case the event should be a full event, which has the `_before` field.

Example:

```yaml
type: "stream"
name: "myapp-mytype-changed"
fromStream: "myapp-mytype-event-full"
pipeline:
- $match:
    $expr:
      $jes-changed:
        pointer: "/myfield"
        from: 0
        to: 1        
```

### $jes-href

The expression of this operator is an object with the fields `app`, `type` and `id`. The latter is optional. All subexpressions should generate a string. The calculated value is the href path.

Example:

```yaml
type: "stream"
name: "mystream"
fromStream: "myapp-mytype-aggregate"
pipeline:
- $set:
    href:
      $jes-href:
        app: "myapp"
        type: "myothertype"
        id: "$otherId"
```

### $jes-name-uuid

The expression of this operator is an object with the mandatory fields `scope` and `key`. The expression of the former should yield a string and that of the latter an integer. The result is a [name-based UUID](https://datatracker.ietf.org/doc/html/rfc4122#section-4.3).

Example:

```yaml
type: "stream"
name: "mystream"
fromStream: "myapp-mytype-aggregate"
pipeline:
- $set:
    otherId:
      $jes-name-uuid:
        scope: "myentity"
        key: "$myotherId"
```

### $jes-removed

The expression of this operator is an object with the field pointer. It should be an expression that yields a JSON pointer. The operator returns true if the pointed field was removed in the event.

Example:

```yaml
type: "stream"
name: "myapp-mytype-removed"
fromStream: "myapp-mytype-event"
pipeline:
- $match:
    $expr:
      $jes-removed: "/myfield"    
```

### $jes-uuid

This operator generates a UUID. It doesn't have an expression.

Example:

```yaml
type: "stream"
name: "mystream"
fromStream: "myapp-mytype-aggregate"
pipeline:
- $set:
    otherId:
      $jes-uuid: null
```