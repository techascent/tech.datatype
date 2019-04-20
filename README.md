# tech.datatype
[![Clojars Project](https://clojars.org/techascent/tech.datatype/latest-version.svg)](https://clojars.org/techascent/tech.datatype)


`tech.datatype` is for efficient manipulation of containers of data.

A nice (slightly out of date) post explaining more is [here](http://techascent.com/blog/datatype-library.html).

Generalized efficient manipulations of sequences of primitive datatype.
Includes specializations for java arrays, array views (subsection of an array)
and nio buffers.  There are specializations to allow implementations to provide
efficient full typed copy functions when the types can be ascertained.

  Generic operations include:
  1. datatype of this sequence
  2. Writing to, reading from
  3. Construction
  4. Efficient mutable copy from one sequence to another
  5. Sparse buffer support
  6. n-dimensional tensor support
  7. Functional math support


design documenation is [here](docs).



## Examples

```clojure
user> (require '[tech.v2.datatype :as dtype])
:tech.resource.gc Reference thread starting
nil
user> (let [ary (dtype/make-container :java-array :float32 (range 10))
            buf (dtype/make-container :native-buffer :float64 10)]
        ;;copy starting at position 2 of ary into position 4 of buf 4 elements
        (dtype/copy! ary 2 buf 4 4)
        ;;buf now has [0 0 0 0 2 3 4 5 0 0]
        (dtype/->vector buf))
Library c found at [:system "c"]
[0.0 0.0 0.0 0.0 2.0 3.0 4.0 5.0 0.0 0.0]
```


* [basic operations](test/tech/v2/datatype_test.clj)
* [unsigned operations](test/tech/v2/typed_buffer_test.clj)
* [game of life](test/tech/v2/apl/game_of_life.clj)
* [sparse game of life](test/tech/v2/apl/sparse_game_of_life.clj)


## License

2Copyright © 2019 TechAscent, LLC.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
