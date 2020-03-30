# DateTime Support


`tech.datatype` now has support for dates, times, and combinations of the two.  Before
getting into the library's explicit support, let's take a quick walk through the
datetime support in java.time and java.time.temporal as datatype's support is mainly
built out of these primitives.



## Base Datatypes


We start with ten datatype divided into three groups.

### Absolute Time Types

Members of the group are absolute datatypes meaning they are fundamentally based on
a time-since epoch model.  In all cases epoch is Jan 1, 1970.  We will refer to a
canonical time, UTC, often.  This stands for Universal Time Coordinated.

These types are:
* `java.time.Instant` - An abstract representation of nanoseconds since epoch.  This
   type has no timezone support.
* `java.time.ZonedDateTime` - An `Instant` combined with a `ZoneId` timezone identifier.
* `java.time.OffsetDateTime` - An `Instant` combined with a fixed offset from UTC.


An `Instant` combined with different ZoneId will yield `ZonedDateTimes` that represent
the same offset from epoch but will print differently and the same thing can be said
of `OffsetDateTimes`.  The reason that we have both `ZonedDateTime` and
`OffsetDateTime` is that one `ZoneId` may indicate different `ZoneOffsets` from UTC
depending on the time of year.


### Local Time Types

The supported local time types are:
* `java.time.LocalDateTime` - A zoneless entity that can store both a date and a time.
* `java.time.LocalDate` - A zoneless entity that can store only a date.  It has no time
   fields.
* `java.time.LocalTime` - A zoneless entity that can only store time - hours, minutes,
   seconds, milliseconds, and nanoseconds.


One way to look at `LocalDateTime` and `LocalDate` is that they have a conversion
to `Instant` - they represent an epoch offset but have no zone related offset.


### Packed Types


We now introduce a few types that are extensions of the above base types that have a
defined primitive size (32 or 64 bits):

* `:packed-instant` - An instant represented by a 64 bit integer that can store time
  down to the millisecond.  This is a fancier version of just storing the 64 bit
  `milliseconds-since-epoch`.
* `:packed-local-date-time` - 64 bit stored value similar to `:packed-instant`.
* `:packed-local-date` - 32 bit entity capable of storing a `LocalDate`.
* `:packed-local-time` - 32 bit entity capable of storing a `LocalTime`.


The main datatypes have a slightly lossy (no nanoseconds) conversion to the packed
datatypes and back.


## Construction, Conversion, and Packing

Now we move to creating the types and converting between them.


### Construction And Type Conversion

All the types have simple constructors that can take no arguments and usually have
a conversion to `milliseconds-since-epoch` and `Instant` and back:

```clojure
user> (require '[tech.v2.datatype :as dtype])
nil
user> (require '[tech.v2.datatype.datetime :as dtype-dt])
nil

user> (def inst (dtype-dt/instant))
#'user/inst
user> inst
#object[java.time.Instant 0x46198608 "2020-03-30T22:41:03.268Z"]
user> (dtype-dt/instant->zoned-date-time inst)
#object[java.time.ZonedDateTime 0x55e5eefd "2020-03-30T16:41:03.268-06:00[America/Denver]"]
user> (dtype-dt/instant->milliseconds-since-epoch inst)
1585608063268
user> (-> (dtype-dt/instant->zoned-date-time inst)
          (dtype-dt/zoned-date-time->milliseconds-since-epoch))
1585608063268
```

In addition, we can also pass in a `ZoneId` or a `ZoneOffset` when converting to/from
instants.  The default zone-id is the `system-zone-id`:

```clojure
user> (dtype-dt/utc-zone-id)
#object[java.time.ZoneRegion 0x4633ca3e "UTC"]
user> (dtype-dt/system-zone-id)
#object[java.time.ZoneRegion 0x735af5f6 "America/Denver"]
user> (dtype-dt/instant->zoned-date-time inst (dtype-dt/utc-zone-id))
#object[java.time.ZonedDateTime 0x3c87218f "2020-03-30T22:41:03.268Z[UTC]"]
user> (dtype-dt/instant->zoned-date-time inst (dtype-dt/system-zone-id))
#object[java.time.ZonedDateTime 0x23ac811f "2020-03-30T16:41:03.268-06:00[America/Denver]"]
```

The local types are accessed the same way:
```clojure
user> (dtype-dt/local-date-time)
#object[java.time.LocalDateTime 0x15d0a532 "2020-03-30T16:46:26.515"]
user> (dtype-dt/local-date-time->instant *1)
#object[java.time.Instant 0x1688b91a "2020-03-30T22:46:26.515Z"]
user> (dtype-dt/local-date-time->milliseconds-since-epoch *2)
1585608386515
```

`LocalTime` objects do you have a conversion to `epoch-milliseconds`.


Likewise, we can create containers of the above types:

```clojure
user> (dtype/make-container :java-array :instant 5)
[#object[java.time.Instant 0x565760c7 "2020-03-30T22:48:22.131Z"],
 #object[java.time.Instant 0x3ccaf9cb "2020-03-30T22:48:22.131Z"],
 #object[java.time.Instant 0x4fac059a "2020-03-30T22:48:22.131Z"],
 #object[java.time.Instant 0x47764c12 "2020-03-30T22:48:22.131Z"],
 #object[java.time.Instant 0x4272228b "2020-03-30T22:48:22.131Z"]]
user> (dtype/make-container :java-array :local-date-time 5)
[#object[java.time.LocalDateTime 0x634b33f1 "2020-03-30T16:48:31.667"],
 #object[java.time.LocalDateTime 0x7bacbbeb "2020-03-30T16:48:31.667"],
 #object[java.time.LocalDateTime 0x44e80624 "2020-03-30T16:48:31.667"],
 #object[java.time.LocalDateTime 0x383d7d88 "2020-03-30T16:48:31.667"],
 #object[java.time.LocalDateTime 0xdbd8375 "2020-03-30T16:48:31.667"]]
user> (dtype/get-datatype *1)
:local-date-time
```


### Packing/UnPacking Types

If a type is packable, we can call pack on an instant, an iterable, or a reader of that
type.  If the call pack on an instance, we get back a base java primitive and have to
explictly call unpack-{type} in order to get back the (slightly lossy) original type.
In the case where we called pack on an iterable or on a reader, however, we can just
call `unpack` -

These function, `pack` and `unpack` follow the conventions to return the basic class
of container, so scalars beget scalars, iterables beget iterables, and readers
beget readers:


```clojure
user> (def inst-container (dtype/make-container :java-array :instant 5))
#'user/inst-container
user> (dtype-dt/pack inst-container)
[568582880711992666 568582880711992666 568582880711992666 568582880711992666 568582880711992666]
user> (dtype/get-datatype *1)
:packed-instant
user> (-> (dtype/make-container :java-array :local-date-time 5)
          (dtype-dt/pack))
[568582880611409804 568582880611409804 568582880611409804 568582880611409804 568582880611409804]
user> (dtype/get-datatype *1)
:packed-local-date-time
user> (-> (dtype/make-container :java-array :local-time 5)
          (dtype-dt/pack))
[272000896 272000896 272000896 272000896 272000896]
user> (dtype/get-datatype *1)
:packed-local-time
user> (dtype-dt/unpack *2)
[#object[java.time.LocalTime 0x292b9237 "16:54:26.496"] #object[java.time.LocalTime 0x64ae72 "16:54:26.496"] #object[java.time.LocalTime 0x52ffff27 "16:54:26.496"] #object[java.time.LocalTime 0x10108d49 "16:54:26.496"] #object[java.time.LocalTime 0x655f1f56 "16:54:26.496"]]
```

Once things are packed, they have a known size and place in memory and you can safely
store them as their raw primitive type.

One useful thing is that if you have a raw untyped container you can change its type
with an `->reader` call:

```clojure
user> (def raw-data [272000896 272000896 272000896 272000896 272000896])
#'user/raw-data
user> (dtype/get-datatype raw-data)
:object
user> (dtype/->reader raw-data :packed-local-time)
[272000896 272000896 272000896 272000896 272000896]
user> (dtype/get-datatype *1)
:packed-local-time
```


## Field Access And Numeric Operators


Here is where the magic is.  Field access and operators all work across all the
defined types above with the same rules for scalar, iterables, and readers.  To get
this support, you need to require `tech.v2.datatype.datetime.operators`.


Not all types support all fields, however, so we have some convenient methods
to show you want types support what fields and what types support what operators:

### Field Table:

```clojure

user> (require '[tech.v2.datatype.datetime.operations :as dtype-dt-ops])
nil
user> (dtype-dt-ops/print-field-compatibility-matrix)

|         :datatype | :day-of-week | :day-of-year | :days | :epoch-days | :epoch-milliseconds | :epoch-seconds | :hours | :iso-day-of-week | :iso-week-of-year | :milliseconds | :minutes | :months | :seconds | :week-of-year | :years |
|-------------------+--------------+--------------+-------+-------------+---------------------+----------------+--------+------------------+-------------------+---------------+----------+---------+----------+---------------+--------|
|          :instant |        false |        false | false |       false |                true |           true |  false |            false |             false |          true |    false |   false |    false |         false |  false |
|       :local-date |         true |         true |  true |        true |                true |          false |  false |             true |              true |         false |    false |    true |    false |          true |   true |
|  :local-date-time |         true |         true |  true |        true |                true |          false |   true |             true |              true |          true |     true |    true |     true |          true |   true |
|       :local-time |        false |        false | false |       false |               false |          false |   true |            false |             false |          true |     true |   false |     true |         false |  false |
| :offset-date-time |         true |         true |  true |        true |                true |           true |   true |             true |              true |          true |     true |    true |     true |          true |   true |
|  :zoned-date-time |         true |         true |  true |        true |                true |           true |   true |             true |              true |          true |     true |    true |     true |          true |   true |
nil
```

### Accessing Fields

All fields are accessed via a `get-{fieldname}` pathway:

```clojure
user> (def ldt-data (dtype/make-container :java-array :local-date-time 5))
#'user/ldt-data
user> (dtype-dt-ops/get-years ldt-data)
[2020 2020 2020 2020 2020]
user> (dtype-dt-ops/get-days ldt-data)
[30 30 30 30 30]
user> (first ldt-data)
#object[java.time.LocalDateTime 0x16a4b70b "2020-03-30T17:04:02.548"]
;;Field access works on packed datatypes if we know the datatype.  In this case
;;we unpack the type and get the field.
user> (-> (dtype-dt/pack ldt-data)
          (dtype-dt-ops/get-days))
[30 30 30 30 30]
user> (dtype-dt-ops/get-epoch-milliseconds ldt-data)
[1585609442548 1585609442548 1585609442548 1585609442548 1585609442548]
```


### Plus,Minus Operator Table

```clojure
user> (dtype-dt-ops/print-plus-op-compatibility-matrix)

|         :datatype | :plus-days | :plus-hours | :plus-milliseconds | :plus-minutes | :plus-months | :plus-seconds | :plus-weeks | :plus-years |
|-------------------+------------+-------------+--------------------+---------------+--------------+---------------+-------------+-------------|
|          :instant |       true |        true |               true |          true |        false |          true |       false |       false |
|       :local-date |       true |       false |              false |         false |         true |         false |        true |        true |
|  :local-date-time |       true |        true |               true |          true |         true |          true |        true |        true |
|       :local-time |      false |        true |               true |          true |        false |          true |       false |       false |
| :offset-date-time |       true |        true |               true |          true |         true |          true |        true |        true |
|  :zoned-date-time |       true |        true |               true |          true |         true |          true |        true |        true |
nil
```

### Plus, Minus Operators

Given a datatype we can add or subtract a given time quantity from it to produce a new
datatype.  Addition is commutative so for addition either the left or right hand side
may be an integer and the other side must be a time quantity.  Subtraction, however is
not so we enforce that subtraction only works if the right-hand side is a number.


```clojure
user> (dtype-dt-ops/plus-days ldt-data (range 5))
[#object[java.time.LocalDateTime 0x56f0dd2e "2020-03-30T17:08:10.620"] #object[java.time.LocalDateTime 0x5a7ee697 "2020-03-31T17:08:10.620"] #object[java.time.LocalDateTime 0x21827569 "2020-04-01T17:08:10.620"] #object[java.time.LocalDateTime 0x623aa11a "2020-04-02T17:08:10.620"] #object[java.time.LocalDateTime 0x327309de "2020-04-03T17:08:10.620"]]
user> (-> (dtype-dt-ops/plus-days ldt-data (range 5))
          (dtype-dt-ops/get-days))
[30 31 1 2 3]
;;Plus, minus also work on scalar values
user> (dtype-dt-ops/plus-days (dtype-dt/zoned-date-time) 5)
#object[java.time.ZonedDateTime 0x519f8ea9 "2020-04-04T17:09:20.794-06:00[America/Denver]"]
```


### Comparison Operators

All datatype support `<. <=, ==, =>, >` assuming both types have the same datatype.

```clojure
user> (def ld-data (vec (repeat 5 (dtype-dt/local-date))))
#'user/ld-data
user> ld-data
[#object[java.time.LocalDate 0x356e503e "2020-03-30"]
 #object[java.time.LocalDate 0x356e503e "2020-03-30"]
 #object[java.time.LocalDate 0x356e503e "2020-03-30"]
 #object[java.time.LocalDate 0x356e503e "2020-03-30"]
 #object[java.time.LocalDate 0x356e503e "2020-03-30"]]
user> (dtype-dt-ops/plus-days ld-data (range 5))
[#object[java.time.LocalDate 0x356e503e "2020-03-30"] #object[java.time.LocalDate 0x57fd4132 "2020-03-31"] #object[java.time.LocalDate 0x66c78e39 "2020-04-01"] #object[java.time.LocalDate 0x39536007 "2020-04-02"] #object[java.time.LocalDate 0x2ab63883 "2020-04-03"]]
user> (def middle-day (dtype-dt-ops/plus-days (dtype-dt/local-date) 2))
#'user/middle-day
user> (dtype-dt-ops/>= middle-day (dtype-dt-ops/plus-days ld-data (range 5)))
[true true true false false]
user> (dtype-dt-ops/== middle-day (dtype-dt-ops/plus-days ld-data (range 5)))
[false false true false false]
```


### Numeric Comparison

In addition to the comparisons above which produce boolean data, we can also take
the difference between two date items and return the result in milliseconds assuming
their types match.


```clojure
user> (def ld-data (dtype/make-container :java-array :offset-date-time 5))
#'user/ld-data
user> ld-data
[#object[java.time.OffsetDateTime 0x5d0b2025 "2020-03-30T17:15:20.134-06:00"],
 #object[java.time.OffsetDateTime 0x3257d91 "2020-03-30T17:15:20.134-06:00"],
 #object[java.time.OffsetDateTime 0x6091af81 "2020-03-30T17:15:20.134-06:00"],
 #object[java.time.OffsetDateTime 0x5bb4fa9a "2020-03-30T17:15:20.134-06:00"],
 #object[java.time.OffsetDateTime 0x14e46045 "2020-03-30T17:15:20.134-06:00"]]
user> (def ranged-data (dtype-dt-ops/plus-seconds ld-data (range 5)))
#'user/ranged-data
user> (dtype-dt-ops/difference-milliseconds ld-data ranged-data)
[0 -1000 -2000 -3000 -4000]
user> (dtype-dt-ops/get-epoch-seconds ranged-data)
[1585610120 1585610121 1585610122 1585610123 1585610124]
user> (dtype-dt-ops/get-epoch-seconds ld-data)
[1585610120 1585610120 1585610120 1585610120 1585610120]
```




## Enough for Now!


We hope this quick run though of the new features of `tech.datatype` will help
when working with date time data.  We also hope this demystified some aspects
of working with the various `java.time` and `java.time.temporal` even (and especially)
when you don't have the datatype library handy.  We are looking forward to expanding
the numerics system with more rich datatypes as time allows.
