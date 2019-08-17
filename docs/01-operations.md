# Operations


[Previously](01-sequences-and-arrays.md) we built up two main abstractions, the first
is an unbounded sequence mechanism and the second is a abstraction (reader) upon
bounded but randomly accessible memory.  In this document we will walk through
transformations from one sequence or reader into another sequence or reader as well
as forcing realization of a given operation.  We then provide some examples of the
performance implications of this design and a real world example using the library.


## The Humble +


>> What do I think about '-'?  I think subtraction is shitty addition.


What is your expectation of the result of `(+ 4 a)`?  Do you expect the result to be a
calculated immediately or do you expect the result to be lazy?  If `a` is a
sequence or a reader, does this change your expectation?


We start with a formulation of + that is constant time regardless of inputs.  We do
this by defining a few rules that apply generically across all operations.  We start by
classifying our inputs into 3 classes `#{:sequence :reader :scalar}`:

1.  If any of the inputs are sequences the result is a sequence.
2.  If any of the inputs are readers the result is a reader.
3.  A scalar is returned (all inputs are scalars).


Given these rules you can see that the output of `(+ a b)` can only be one of the three
classes.

There are a couple transformations we should note:

*  A scalar can be transformed into a sequence of unbounded length or a reader in
constant time by creating an implementation of the interface that just repeatedly
returns the same scalar value.
*  A reader can be transformed into sequence in constant time.


Thus show how to implement `(+ 4 a)` in constant time we walk through the different
classes that 'a' could be:

* `scalar` - add 4 to the value and return the value.
* `sequence` - Create a new sequence that upon realization of each item adds 4 to the
   source sequence item.
* `reader` - Create a new reader that upon indexing adds 4 to the source reader.


Thus we implement `(+ 4 a)` in constant time regardless of the type or length of `a`:

```clojure
user> (require '[tech.v2.datatype :as dtype])
nil
user> (require '[tech.v2.datatype.functional :as dfn])
nil
user> (dfn/+ 4 4)
8
user> (dfn/+ 4 (list 1 2 3 4 5))
(5 6 7 8 9)
user> (instance? tech.v2.datatype.ObjectReader *1)
false
user> (instance? Iterable *2)
true
user> (dfn/+ 4 [1 2 3 4 5])
[5 6 7 8 9]
user> (instance? tech.v2.datatype.ObjectReader *1)
true
user> (instance? Iterable *2)
true
```

In the example above, we should talk just briefly about some Clojure constructs and
their implications:

*  The expression `(list 1 2 3 4 5)` produces a Clojure list.  This is a sequence
datastructure and you can see that the result of adding a number to this sequence
is represented in the repl by a new sequence because the sequence boundaries are
denoted by parenthesis - `(5 6 7 8 9)`.

*  The expression `[1 2 3 4 5]` produces a Clojure persistent vector.  This implements
two important interfaces:  `java.util.List` and `java.util.RandomAccess` and thus is
interpreted by the system as a reader.  Hence the result is displayed in the repl as
`[5 6 7 8 9]` but the result isn't a persistent vector; it is a reader.  The repl
itself displays anything that derives from `RandomAccess` (which all readers do)
the same way as a persistent vector which makes sense; they both allow randomly indexed
access to their data.


## Laziness


The result above is constant time regardless of the length of `a`:

```clojure
user> (def a (double-array 1000))
#'user/a
user> (def b (time (dfn/+ 4 a)))
"Elapsed time: 0.894957 msecs"
#'user/b
user> (def a (double-array 10000000))
#'user/a
user> (def b (time (dfn/+ 4 a)))
"Elapsed time: 0.693158 msecs"
#'user/b
(def a (double-array 100000000))
#'user/a
user> (def b (time (dfn/+ 4 a)))
"Elapsed time: 0.743744 msecs"
#'user/b
```


This is because b is never evaluated.  A construction of 'b' is created that is
a new reader that points back to the original 'a' and the scalar.  The interaction
of sequences, iterators and readers with respect to laziness is interesting:

* `sequences` - lazy, caching
* `iterators` - lazy, non-caching
* `readers` - lazy, non-caching

So one would expect that realizing all of the indexes of a reader to take the same
amount of time every time.  In the example above, and iteration through all of the
indexes of 'b' will take about the same time if you do it twice or 100 times.


## Chaining Readers


What do you suppose happens if we apply the operation again to 'b'?  This will
create new reader:

```clojure
user>  (def c (-> [1 2 3 4 5]
                  (dfn/+ 4)
                  (dfn/+ 4)))
#'user/c
user> c
[9 10 11 12 13]
```

The result, 'c', is a reader.  So until the repl printed the result, 'c' wasn't
realized and was in fact a reader that pointed to another reader that pointed back
to the original persistent vector.  All of this is still constant time until
realization.  Reader chains can be unbounded and elements are evaluated upon read
of a given index.


An important side effect of this is that the temporaries created are data definitions,
not concrete datastructures.  So long chains of readers do not create temporaries.

Consider this expression coming from a [kaggle tutorial](https://nbviewer.jupyter.org/github/cnuernber/ames-house-prices/blob/82e3ce1679b3e6e31c0128290f60ef7ae16947b0/ames-housing-prices-clojure.ipynb):

```clojure
(dfn/+ (col "BsmtFullBath") (dfn/* 0.5 (col "BsmtHalfBath"))
       (col "FullBath") (dfn/* 0.5 (col "HalfBath")))
```

With non-lazy functional math (of the type that numpy performs), how many temporary
allocations of potentially large vectors are required?  Well, one for each pairwise
operation:

1.  `(dfn/* 0.5 (col "HalfBath"))`
2.  `(dfn/* 0.5 (col "BsmtHalfBath"))`
3.  `(dfn/+ (col "FullBath") #1)`
4.  `(dfn/+ (col "BsmtFullBath" #2)`
5.  `(dfn/+ #3 #4)`


With lazy chaining we create lazy readers in place of temporary double arrays and
thus we dramatically reduce the number of large block allocation/deallocation
operations.  The side effect of this laziness is that we have to have an explicit
'realize' step.


## Realizing the Result


The question becomes, then, how to realize the result.  Since readers are convertible
to sequences one way is to use the Clojure base datatype constructors:

```clojure
user> (vec c)
[9 10 11 12 13]
```

Or we can use the concrete array constructors:
```clojure
user> (double-array c)
[9.0, 10.0, 11.0, 12.0, 13.0]
user> (int-array c)
[9, 10, 11, 12, 13]
```

These will realize the reader as if it were a sequence.  A faster way by quite a bit
is to use the datatype library's copy mechanism:

```clojure
user> (def a (double-array 10000000))
#'user/a
user> (def b (-> a
                 (dfn/+ 4)
                 (dfn/* 2)))
#'user/b
user> (def c (time (vec b)))
"Elapsed time: 888.144842 msecs"
#'user/c
user> (dtype/get-datatype b)
:float64
user> (def c (time (dtype/make-container :java-array :float64 b)))
"Elapsed time: 192.747839 msecs"
#'user/c
(def c (time (dtype/->array-copy b)))
"Elapsed time: 190.568381 msecs"
#'user/c
user> (type c)
[D
user> (def c (double-array (dtype/ecount b)))
#'user/c
user> (def ignored (time (dtype/copy! b c)))
"Elapsed time: 184.246553 msecs"
#'user/ignored
```


## Conclusion


Above we walk through how the datatype library implements transformations on scalars,
sequences and readers.  We call these transformations 'operations' and we walk through
the actual implications of making them constant time.  We assert that having
elementwise operations across large collections of primitives be lazy allows
functional programming constructs to be more competitive when deep composition is
involved than a naive implementation that produces a new large allocation per
mathematical operation or even worse, boxes and then unboxes each primitive.


Understanding this much allows you to perform elementwise operations across sequences,
persistent vectors, java arrays and native buffers.  In fact, you can write one routine
that will work with all of the above and can produce a result in terms of all of the
above across all the datatypes that tech.datatype supports.  With an understanding
of some of the performance impacts you can write code that is generally efficient
enough with a very easy pathway to making it quite efficient should the need arise.


#### Addendum 1 - Performance Impacts Of Chaining


Reader chaining has some interesting performance implications.  The example above's
running time doesn't change much if we add more operations:

```clojure
user> (def b (-> a
                 (dfn/+ 4)
                 (dfn/* 2)
                 (dfn/- 3)
                 (dfn// 2)))
#'user/b
user> (def ignored (time (dtype/copy! b c)))
"Elapsed time: 333.823614 msecs"
#'user/ignored
user> (def b (-> a
                 (dfn/+ 4)
                 (dfn/* 2)
                 (dfn/- 3)
                 (dfn// 2)
                 (dfn/+ 6)
                 (dfn/* 4)))
#'user/b
user> (def ignored (time (dtype/copy! b c)))
"Elapsed time: 482.554133 msecs"
#'user/ignored
user>
```

Consider the performance impact if every operation allocated a temporary
80MB of double numbers.  Then every operation contains an allocation and a write
step and with that much data you overwhelm the cache on every operation in addition
to writing out to main memory and then attempting to read it back.


The fastest way to do this will be some level of custom programming where you perform
the elementwise operations inline:

```clojure
user> (def b (reify tech.v2.datatype.DoubleReader
               (lsize [this] (dtype/ecount a))
               (read [this idx]
                 (-> (aget a idx)
                     (+ 4)
                     (* 2)
                     (- 3)
                     (/ 2)
                     (+ 6)
                     (* 4)))))
#'user/b
user> (def ignored (time (dtype/copy! b c)))
"Elapsed time: 35676.376985 msecs"
#'user/ignored
```

This is difficult to get right.  In the example above we used 'a' which is a 'def'.
'def' variables are opaque types to the compiler so we help it out by hinting at the
exact definition of 'a':

```clojure
user> (def b (let [^doubles a a]
               (reify tech.v2.datatype.DoubleReader
                 (lsize [this] (dtype/ecount a))
                 (read [this idx]
                   (-> (aget a idx)
                       (+ 4)
                       (* 2)
                       (- 3)
                       (/ 2)
                       (+ 6)
                       (* 4))))))
#'user/b
user> (def ignored (time (dtype/copy! b c)))
"Elapsed time: 68.395993 msecs"
#'user/ignored
```

But as you can see, doing this and getting all the typehinting to work correctly is
quite tough.  And the code is specific to the container type of 'a' so that if we were
to externally change the container or the datatype our code would break.  On the other
hand, we get nearly 8 times faster *if* everything (types, datastructures, etc) line
up.


As with anything, moving from reader composition to inline reader operations is an
optimization that shouldn't be done until it is necessary.  But if you need absolute
maximum performance then the option is there.  An interesting intermediate step
would be if one could transform a reader chain into a compiled operation at runtime.


#### Addendum 2 - Real World Example - Weather Forecasting


Weather forecast engines produce forecasts at fixed intervals, so for instance once an
hour for the next 16 hours.  Given a lat/lon/time triple, we want to linearly
interpolate the two nearest engine forecasts to produce a relatively exact forecast
for times that are in between the fixed intervals.

The engines can produce data that is degrees from true north, however.  This data
also needs to be lerped but a naive lerp will not produce the correct answer because
the average of 350 and 10 is 180 which is exactly opposite of the correct direction.
So for degrees, we reify a custom reader that does the operation upon request of a
specific index.

In the code below you can see the progression from the straightforward linear
interpolation to the specialized degrees-from-north based linear interpolation.


```clojure
(defn- lerp-tensor-result
  "slice will return a sequence of vectors of [n-items] length and
apply + will sum elementwise across them.  Nothing yet is
realized and no work has been done.  Then we do all the work at
once with a copy to a known datatype (double-array)"
  [final-tensor]
  (let [tens-shape (dtype/shape final-tensor)]
    (dtype/copy!
     (apply dfn/+ (dtt/slice final-tensor 1))
     (double-array (second tens-shape)))))


(defn- lerp-latest-realize-records
  [{:keys [name level data-table] :as variable}
   records]
  (let [records (vec records)
        first-record (first records)
        partition-key (:partition-key first-record)
        lat-lon-seq (mapv :lat-lon-tuple records)
        ;;there are y-weights partition keys and n-items records
        weights (->
                 ;;not needing to count makes things go faster
                 (mapv (comp (partial mapv first) :lerp-info)
                       records)
                 ;;this isn't very efficient but will work
                 (dtt/->tensor)
                 ;;[n-items y-weights] -> [y-weights n-items]
                 ;;transpose is in place
                 (dtt/transpose [1 0]))
        ;;Creates a value tensor of [y-weights n-items] of the data
        values (->> partition-key
                    (map #(last-lookup-lat-lon data-table %
                                               lat-lon-seq))
                    ;;The tensor constructor will efficiently copy
                    ;;the nested sequence of double arrays into the
                    ;;backing store of the tensor
                    (dtt/->tensor))]
    ;;lazy elementwise mul returning [y-weights n-items]
    (-> (dfn/* weights values)
        (lerp-tensor-result))))

(defn- clamp-degrees
  ^double [^double value]
  (if (or (>= value 360.0)
          (< value 0))
    (recur (+ value
              (if (< value 0)
                360.0
                -360.0)))
    value))


(defn- degree-2d-lerp
  [values weights]
  (let [[lhs rhs] (dtt/slice values 1)
        [lhs-weights rhs-weights] (dtt/slice weights 1)
        ;;Type everything out so we can efficiently produce a new reader
        lhs (typecast/datatype->reader :float64 lhs)
        rhs (typecast/datatype->reader :float64 rhs)
        lhs-weights (typecast/datatype->reader :float64 lhs-weights)
        rhs-weights (typecast/datatype->reader :float64 rhs-weights)
        n-elems (dtype/ecount lhs)]
    (reify
      DoubleReader
      (lsize [rdr] n-elems)
      (read [rdr idx]
        (let [lhs (.read lhs idx)
              rhs (.read rhs idx)
              lhs-w (.read lhs-weights idx)
              rhs-w (.read rhs-weights idx)
              [lhs rhs] (if (> (Math/abs (- lhs rhs)) 180.0)
                          (if (< lhs rhs)
                            [(+ lhs 360) rhs]
                            [lhs (+ rhs 360)])
                          [lhs rhs])]
          (clamp-degrees (+ (* (double lhs) lhs-w)
                            (* (double rhs) rhs-w))))))))


(defn- degree-lerp-latest-realize-records
  [{:keys [name level data-table] :as variable}
   records]
  (let [records (vec records)
        first-record (first records)
        partition-key (:partition-key first-record)
        lat-lon-seq (mapv :lat-lon-tuple records)
        ;;there are y-weights partition keys and n-items records
        weights (->
                 ;;not needing to count makes things go faster
                 (mapv (comp (partial mapv first) :lerp-info)
                       records)
                 ;;this isn't very efficient but will work
                 (dtt/->tensor)
                 ;;[n-items y-weights] -> [y-weights n-items]
                 ;;transpose is in place
                 (dtt/transpose [1 0]))
        ;;Creates a value tensor of [y-weights n-items] of the data
        values (->> partition-key
                    (map #(last-lookup-lat-lon data-table %
                                               lat-lon-seq))
                    ;;The tensor constructor will efficiently copy
                    ;;the nested sequence of double arrays into the
                    ;;backing store of the tensor
                    (dtt/->tensor))
        n-weights (long (first (dtype/shape weights)))
        ;;Exception if more than 2
        weighted-values (case n-weights
                          1 values
                          2 (degree-2d-lerp values weights))]
    (comment (println
              "\nvalues:" (dtt/->jvm values)
              "\nweights:" (dtt/->jvm weights)
              "\nresult:" (vec weighted-values)))
    ;;Make weighted values an appropriately shaped tensor
    (-> (dtt/reshape weighted-values
                     [1 (dtype/ecount weighted-values)])
        (lerp-tensor-result))))
```
