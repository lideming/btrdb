# `binval` format

binval (binary value) is yet another BSON-like format to encode JavaScript
values into binary.

It's more space-efficient than BSON. It saves the space by encoding the length
into the "type" byte in many cases.

See [binval.ts](../src/binval.ts) for the implementation.

## Value

```
(1 byte) value type
(?) optinal value body depends on type
```

| type      | JS value                                      |
| --------- | --------------------------------------------- |
| 0         | `null`                                        |
| 1         | `undefined`                                   |
| 2         | `false`                                       |
| 3         | `true`                                        |
| 4         | (body) uint8                                  |
| 5         | (body) uint16                                 |
| 6         | (body) uint32                                 |
| 7         | (body) -uint8                                 |
| 8         | (body) -uint16                                |
| 9         | (body) -uint32                                |
| 10        | (body) float64                                |
| 11        | (body) length-prefixed string                 |
| 12        | (body) length-prefixed binary data            |
| 13        | (body) object with length-prefixed prop count |
| 14        | (body) array with length-prefixed item count  |
| 15 ~ 35   | (no used yet)                                 |
| 36 ~ 44   | (body) array with item count 0 ~ 8            |
| 45 ~ 53   | (body) object with key count 0 ~ 8            |
| 54 ~ 86   | (body) binary data with length 0 ~ 32         |
| 87 ~ 119  | (body) string with length 0 ~ 32              |
| 120 ~ 127 | number `-7` ~ `-0`                            |
| 128 ~ 255 | number `+0` ~ `127`                           |

## The length-prefixed

```
(1 byte) uint8 small_value
if (small_value == 254)
    (2 byte) uint16 true_value
else if (small_value == 255)
    (4 byte) uint32 true_value
```

## Object

```
if (use length-prefixed)
    (n byte) length-prefixed prop_count
for each prop
    (n byte) length-prefixed property key string
    (n byte) the property value
```

## Array

```
if (use length-prefixed)
    (n byte) length-prefixed item_count
for each item
    (n byte) the item value
```
