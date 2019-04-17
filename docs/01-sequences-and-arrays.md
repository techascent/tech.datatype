# Sequences And Arrays


## Turing Machines and LISP


We start our discussion talking about the basis of what is computable and why.  In the
beginning, there was the [Turing Machine](https://en.wikipedia.org/wiki/Turing_machine)
and it was good.  No one built an exact turing machine of course but the axioms developed
for doing computions using a turing machine have proved to have a universal truth about
them.


The first turing machine design had tape that you could scroll forward or backward and a
finite state machine that controlled reading, scrolling, and moving this tape.  This was
sufficient to describe all of the computations that we know about as a civilization in
an abstract sense.  This first turing machine relates closes to the type of sequence
based programming we find in normal course of functional programming.  Whether your
favorite language is typed or not the base mechanics of how you do your computation are
the same.  Generally speaking, you have sequences, you have map, filter, and reduce.
Adding in some minimal control logic and variable declaration and many computational
problems seem to disolve into fairly simple blocks of the above operations.  We can add
distinct (if we have some form of scalar memory along with sets) and group-by to this
system but we cannot in fact efficiently add sort.


John McCarthy came along and in 1955 and invented LISP which is I think a beautiful
language because it is based on simple principles build a complete turing machine.  He
had the insight to build the compiler in LISP and to make the boundaries between the
compiler and the program blurry so that you could flow between programming the actual
machine and extending the language itself via programming the compiler.  LISP is
`homoiconic` which means the language is completely described using datastructures and
elements that exist within the language thus making programming the compiler via reading
source code and transforming it is the same as reading in data to the program and
transforming it via normal programming routines.


## Math And Notation


Around the same time that McCarthy was working in LISP, Ken Iverson was getting a PhD
from Harvard in mathematics.  Ken was working under a legendary visionary and educator
named Howard Aiken.  Within months of finishing his PhD, Ken was asked by Aiken to
prepare a course on doing business data processing.


During the course development of the course, Ken realized that the mathematical notation
that he was using was lacking in expressiveness so he designed new notation.  He then
failed to get tenure at Harvard because in 5 years he had only published "one little book."
Later, hhile working at IBM and published two books: "A Programming Language" and
"Automatic Data Processing."  He and a team completed an initial working batch-mode
implementation of the language in 1965 and a timeshare version in 1967.  He received
the Turing Award in 1979 and continued on to make the opensource J programming language.


APL is based on array programming; so you are dealing with contiguous groups items of a
fixed size.  This has important implications and benefits.  In fact, there are versions
of Turing machines that have random access memory and you can show the two types of
machines equivalent but in practice the two types of programming, sequence vs. array,
tend to have different shapes.


The foundation of APL (A Programming Language) appears to be nearly completely different
than LISP but I intend to show a natural progression from the familiar sequence based
constructs of LISP that we all love to the N-dimensional constructs of APL-type
languages.


One thing to keep in mind throughout all of this is the fact that regardless of how
advanced the hardware underlying your computation system is, the thing it would like to
be doing the most is the same thing, over and over again.  APL had amazing performance
working in very constrained environments precisely because the language is structured
such that you do operations on rectangles of data instead of on one datum.


## Toward Unification


### Sequences


Throughout this discussion you will not see any language based security directives
(public, private) as they are entirely irrelevant to this discussion.

We start by defining a what a sequence is.  A sequence is an abstract interface that has
two members, current and next.    Current points to the current object
in the sequence and next points to another sequence interface or nothing.

```java
interface SequenceElement
{
  Object current();
  SequenceElement next();
}

interface Sequence
{
  SequenceElement first();
}
```

Should we get into a situation where we need to avoid boxing things, we have no choice
but move to a mutable iterator.  It should be noted that a mutable iterator is the
clumsy approximation to a sequence that is forced by the necessity to deal with concrete
types directly for efficiency concerns:

```java
interface ByteIterator
{
  byte current();
  bool hasNext();
  byte nextByte();
}

interface Iterable
{
  ByteIterator iterator();
}
```

Note that we attempt the most minimal transformation from sequences to iterators.  Note
also that the iterator has a `current` member.  This allows a dramatic simplification of
algorithms as compared to using only `nextByte` as the algorithm itself does not need to
store both the iterator and the current item.  It also matches our sequence definition
more precisely leading to a natural progression from sequences to iterators where
efficiency concerns are beginning to dominate.


Given these two definitions we can expect a few things:

* map, filter, etc. have lazy processing semantics
* count realises the entire sequence
* sort is not possible without another piece of engineering
* reverse is O(N)


### Arrays


We now abstract the concept of an array into 2 interfaces, readers and writers.

```java
interface Countable
{
  int size();
}

interface ByteReader extends Countable
{
  byte read(int idx);
}

interface ByteWriter extends Countable
{
  void writer(int idx, byte value);
}
```

In addition to these strongly typed interfaces, there are weakly typed bulk
interfaces:

```clojure
(defprotocol PSetConstant
  (set-constant! [item offset value elem-count]))

(defprotocol PWriteIndexes
  (write-indexes! [item indexes values options]))

(defprotocol PReadIndexes
  (read-indexes! [item indexes values options]))
```

Both share some level of interface and we have some expectations about them.  For a lot
of reasons, we use strongly typed interfaces where we have an efficiency concern and
weakly typed interfaces elsewhere.  Chatty, strongly typed interfaces are less ideal
than bulk interfaces but we cannot always avoid using them.  The peak performance of a
chatty interface given the most sophisticated optimizations we know of is still far
below the peak performance of a bulk interface regardless of typing if for nothing else
than we have to pay for the cost of crossing the interface a large number of times.


We don't explicitly state anything about where read or write are getting/putting their
data allowing us to map those interfaces to anything we like.

The reader concept is a natural extension of the sequence concept that allows
us to implement algorithms that imply random access generically.

* size is constant time
* reverse is constant time
* We now have binary-search, which gives us a rough version of sort
* Analog of map is reader-map, which does the operation on read of each element
* Reductions still only require iterators, creating an iterator from a reader is a
constant time operation
* Parallelizing operations across a range of integers is far easier (and more efficient)
than implementing any form of parallel map
* Given data and index readers, we can return a new reader that arbitrarily changes the
order of the values in the original data reader
* Concatenation produces a new reader in constant time


Moving to more sophisticated operations, we build `arg` versions of our
usual paradigms.  Arg means the operation returns results in index space.:

* `binary-search` - return tuple of `[found? index]` where if found is true, index
points to where the element is and if found is false, index points to where one would
call `insert` to produce a sorted sequence
* `argsort` - return sorted list of indexes
* `argfilter` - return a potentially shorter list of indexes based on a boolean
  operation
* `argmax, argmin, argcompare` - Return index of the the min, max, item that compared
the highest as compared to the rest of the items


## Mutables

An object that knows how to insert elements and is convertible in constant time to a
reader that reads memory contiguously and a writer that writes to that same block of
memory. This is neither a list nor a vector but Java called it a list and vector is
heavily overloaded with other semantic definitions.  `insert` can be called on the
return value of `size` and the result is the addition of the value to the end of
the mutable.


```java
interface MutableRemove
{
  void remove(int idx);
}

interface ByteMutable extends MutableRemove
{
  void insert(int index, byte value);
}
```

In addition to the strongly typed interface, we have bulk weakly typed interfaces:

```clojure
(defprotocol PRemoveRange
  (remove-range! [item idx n-elems]))

(defprotocol PInsertBlock
  (insert-block! [item idx values options]))
```



## Operators

map isn't useful without a function that transforms each element to a different element.
Functional programming isn't useful without higher level operators.
