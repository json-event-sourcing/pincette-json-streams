# Query Operators

This page lists the supported [MongoDB query and projection operators](https://docs.mongodb.com/manual/reference/operator/query/), with possible deviations.

## Comparison

### [$eq](https://docs.mongodb.com/manual/reference/operator/query/eq/#mongodb-query-op.-eq)
Example:

```yaml
myfield:
  $eq: "test"
```

This can also be written as:

```yaml
myfield: "test"
```

### [$gt](https://docs.mongodb.com/manual/reference/operator/query/gt/#mongodb-query-op.-gt)
Example:

```yaml
myfield:
  $gt: 100
```

### [$gte](https://docs.mongodb.com/manual/reference/operator/query/gte/#mongodb-query-op.-gte)

Example:

```yaml
myfield:
  $gte: 100
```

### [$in](https://docs.mongodb.com/manual/reference/operator/query/in/#mongodb-query-op.-in)
Example:

```yaml
myfield:
  $in:
  - 1
  - 2
  - 3
```

### [$lt](https://docs.mongodb.com/manual/reference/operator/query/lt/#mongodb-query-op.-lt)
Example:

```yaml
myfield:
  $lt: 100
```

### [$lte](https://docs.mongodb.com/manual/reference/operator/query/lte/#mongodb-query-op.-lte)

Example:

```yaml
myfield:
  $lte: 100
```

### [$ne](https://docs.mongodb.com/manual/reference/operator/query/ne/#mongodb-query-op.-ne)
Example:

```yaml
myfield:
  $ne: 10
```

### [$nin](https://docs.mongodb.com/manual/reference/operator/query/nin/#mongodb-query-op.-nin)

Example:

```yaml
myfield:
  $nin:
  - 1
  - 2
  - 3
```

## Logical

### [$and](https://docs.mongodb.com/manual/reference/operator/query/and/#mongodb-query-op.-and)

Example:

```yaml
$and:
- x:
    $gte: 0
- y:
    $gte: 0
```

### [$not](https://docs.mongodb.com/manual/reference/operator/query/not/#mongodb-query-op.-not)

Example:

```yaml
myfield:
  $not:
    $lt: 0
```

### [$nor](https://docs.mongodb.com/manual/reference/operator/query/nor/#mongodb-query-op.-nor)

Example:

```yaml
$nor:
- x:
    $lt: 0
- y:
    $lt: 0
```

### [$or](https://docs.mongodb.com/manual/reference/operator/query/or/#mongodb-query-op.-or)

Example:

```yaml
$or:
- x:
    $gt: 0
- y:
    $gt: 0
```

## Element

### [$exists](https://docs.mongodb.com/manual/reference/operator/query/exists/#mongodb-query-op.-exists)

Example:

```yaml
myfield:
  $exists: true
```

### [$type](https://docs.mongodb.com/manual/reference/operator/query/type/#mongodb-query-op.-type)

Example:

```yaml
myfield:
  $type: "string"
```

## Evaluation

### [$expr](https://docs.mongodb.com/manual/reference/operator/query/expr/#mongodb-query-op.-expr)

The MongoDB server only allows this operator at the top-level of an expression. This restriction doesn't apply in streaming mode, because no complicated query plan is needed. So, you can use it at any level of nesting, provided that the wrapped expression can be evaluated as a Boolean.

Example:

```yaml
$expr:
  $gt:
  - "$field1"
  - "$field2"
```

### [$mod](https://docs.mongodb.com/manual/reference/operator/query/mod/#mongodb-query-op.-mod)

Example:

```yaml
myfield:
  $mod:
  - 5
  - 1
```

### [$regex](https://docs.mongodb.com/manual/reference/operator/query/regex/#mongodb-query-op.-regex)

Example:

```yaml
myfield:
  $regex: "/^test/"
  $options: "i"
```

This can be written shorter as:

```yaml
myfield: "/^test/i"
```

## Array

### [$all](https://docs.mongodb.com/manual/reference/operator/query/all/#mongodb-query-op.-all)

The `$elemMatch` operator is not supported within the `$all` operator.

Example:

```yaml
myfield:
  $all:
  - 1
  - 2
  - 3
```

### [$elemMatch](https://docs.mongodb.com/manual/reference/operator/query/elemMatch/#mongodb-query-op.-elemMatch)

Example:

```yaml
myfield:
  $elemMatch:
    name: "test"
```

### [$size](https://docs.mongodb.com/manual/reference/operator/query/size/#mongodb-query-op.-size)

Example:

```yaml
myfield:
  $size: 3
```

## Bitwise

### [$bitsAllClear](https://docs.mongodb.com/manual/reference/operator/query/bitsAllClear/#mongodb-query-op.-bitsAllClear)

Example:

```yaml
myfield:
  $bitsAllClear:
  - 2
  - 3
```

### [$bitsAllSet](https://docs.mongodb.com/manual/reference/operator/query/bitsAllSet/#mongodb-query-op.-bitsAllSet)

Example:

```yaml
myfield:
  $bitsAllSet:
  - 2
  - 3
```

### [$bitsAnyClear](https://docs.mongodb.com/manual/reference/operator/query/bitsAnyClear/#mongodb-query-op.-bitsAnyClear)

Example:

```yaml
myfield:
  $bitsAnyClear:
  - 2
  - 3
```

### [$bitsAnySet](https://docs.mongodb.com/manual/reference/operator/query/bitsAnySet/#mongodb-query-op.-bitsAnySet)

Example:

```yaml
myfield:
  $bitsAnySet:
  - 2
  - 3
```

## Miscellaneous

### [$comment](https://docs.mongodb.com/manual/reference/operator/query/comment/#mongodb-query-op.-comment)

Example:

```yaml
x:
  $mod:
  - 2
  - 0
$comment: "Select even values"  
```