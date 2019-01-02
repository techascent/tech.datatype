# tech.datatype 
[![Clojars Project](https://clojars.org/techascent/tech.datatype/latest-version.svg)](https://clojars.org/techascent/tech.datatype)


A Clojure library for efficient manipulation of contiguous mutable containers of primitive datatypes.

A nice post explaining more is [here](http://techascent.com/blog/datatype-library.html).

Generalized efficient manipulations of sequences of primitive datatype.
Includes specializations for java arrays, array views (subsection of an array)
and nio buffers.  There are specializations to allow implementations to provide
efficient full typed copy functions when the types can be ascertained.

  Generic operations include:
  1. datatype of this sequence.
  2. Writing to, reading from.
  3. Construction.
  4. Efficient mutable copy from one sequence to another.



## Examples


```clojure
(:require [tech.datatype.core :as dtype])

(let [ary (dtype/make-array-of-type :float32 (range 10))
      buf (dtype/make-array-of-type :float64 10)]
  ;;copy starting at position 2 of ary into position 4 of buf 4 elements
  (dtype/copy! ary 2 buf 4 4))
  ;;buf now has [0 0 0 0 2 3 4 5 0 0]
```

## License

Copyright © 2018 TechAscent, LLC.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
