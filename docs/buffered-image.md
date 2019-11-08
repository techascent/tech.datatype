# Buffered Image Support


`tech.datatype` contains support for loading/saving buffered images
and creating tensors from buffered images.


## Usage


Buffered images implement the protocols required to be part of the datatype system.

### Basics



```clojure

user> (require '[tech.v2.datatype :as dtype])
nil
user> (require '[tech.libs.buffered-image :as bufimg])
nil
user> (def test-img (bufimg/load "../tech.opencv/test/data/test.jpg"))
#'user/test-img
user> test-img
#object[java.awt.image.BufferedImage 0x71374470 "BufferedImage@71374470: type = 5 ColorModel: #pixelBits = 24 numComponents = 3 color space = java.awt.color.ICC_ColorSpace@27f821b6 transparency = 1 has alpha = false isAlphaPre = false ByteInterleavedRaster: width = 512 height = 288 #numDataElements 3 dataOff[0] = 2"]

user> (dtype/get-datatype test-img)
:uint8
user> (dtype/shape test-img)
[288 512 3]
user> (bufimg/image-channel-format test-img)
:bgr

user> (require '[tech.v2.tensor :as dtt])
nil

;;It is safe to print tensors to repl.  They will elide data in a similar fashion
;;as numpy arrays.
user> (dtt/ensure-tensor test-img)
#tech.v2.tensor<uint8>[288 512 3]
[[[172 170 170]
  [172 170 170]
  [171 169 169]
  ...
  [ 24  18  23]
  [ 24  18  23]
  [ 24  18  23]]
  ...

;;You can get a fully-typed reader from a tensor to read indiviudal bytes:
user> (def test-rdr (tens-typecast/datatype->tensor-reader :uint8 (dtt/ensure-tensor test-img)))
#'user/test-rdr
user> (.read3d test-rdr 0 0 1)
170
user> (.read3d test-rdr 0 0 2)
170
user> (.read3d test-rdr 0 0 3)
172
```

* One important thing to note is the return value of ensure-tensor shares the backing
data store with the source object.  So you can write to the tensor with any of
the datatype methods and the result will be written into the buffered image.


### Different Image Types


```clojure


;;If you have images of other base storage types you may get a different tensor than
;;you want:

user> (vec(keys bufimg/image-types))
[:int-bgr
 :byte-gray
 :byte-binary
 :ushort-gray
 :ushort-555-rgb
 :int-argb-pre
 :byte-indexed
 :custom
 :byte-bgr
 :byte-abgr-pre
 :int-rgb
 :ushort-565-rgb
 :byte-abgr
 :int-argb]

user> (def test-img (bufimg/new-image 4 4 :int-bgr))
#'user/test-img
user> (dtt/ensure-tensor test-img)
#tech.v2.tensor<int32>[4 4 1]
[[[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]
 [[0]
  [0]
  [0]
  [0]]]

;;So we have a convenience method that will always return a uint8 tensor:

user> (bufimg/as-ubyte-tensor test-img)
#tech.v2.tensor<uint8>[4 4 3]
[[[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]
 [[0 0 0]
  [0 0 0]
  [0 0 0]
  [0 0 0]]]

```

### Mutating The Image


```clojure

;;You can use this as you can any other tensor.  See the cheatsheet.

(def img-writer (tens-typecast/datatype->tensor-writer
                       :uint8
                        (bufimg/as-ubyte-tensor test-img)))
#'user/img-writer
user> (.write3d img-writer 2 2 1 255)
nil
user> (dtype/->reader test-img)
[0 0 0 0 0 0 0 0 0 0 65280 0 0 0 0 0]

user> (bufimg/as-ubyte-tensor test-img)
#tech.v2.tensor<uint8>[4 4 3]
[[[0   0 0]
  [0   0 0]
  [0   0 0]
  [0   0 0]]
 [[0   0 0]
  [0   0 0]
  [0   0 0]
  [0   0 0]]
 [[0   0 0]
  [0   0 0]
  [0 255 0]
  [0   0 0]]
 [[0   0 0]
  [0   0 0]
  [0   0 0]
  [0   0 0]]]

```

### Tensor Operations

All the tensor operations return data in-place.  So in the transpose call below a view
is returned without actually doing any copies.  Iterating over tensors iterates
over the outermost dimension returning a sequence of tensors or a sequence of numbers
if this is a one dimension tensor.


Our image is in BGR-interleaved format, so what we first do is transpose the image into
BGR planar format.  We then use the statistical methods in datatype in order to get
the per-channel statistics for the image.

```clojure

;;Having images be tensors is useful for a few things, but stats is one of them.

user> (def planar-tens (dtt/transpose test-tens [2 0 1]))
#'user/planar-tens
user> planar-tens
#tech.v2.tensor<uint8>[3 288 512]
[[[172 172 171 ... 24 24 24]
  [173 174 173 ... 23 23 23]
  [174 175 174 ... 23 22 22]
  ...
  [ 37  37  37 ... 55 54 55]
  [ 36  35  35 ... 52 50 53]
  [ 39  37  34 ... 51 49 53]]
 [[170 170 169 ... 18 18 18]
  [171 172 171 ... 17 17 17]
  [172 173 172 ... 17 16 16]
  ...
  [ 46  46  46 ... 51 50 51]
  [ 45  44  44 ... 51 49 52]
  [ 48  46  43 ... 50 48 52]]
 [[170 170 169 ... 23 23 23]
  [171 172 171 ... 22 22 22]
  [172 173 172 ... 22 21 21]
  ...
  [ 84  84  83 ... 56 55 56]
  [ 83  82  81 ... 55 53 56]
  [ 86  84  80 ... 54 52 56]]]
user> (require '[tech.v2.datatype.functional :as dfn])
nil
user> (map dfn/descriptive-stats planar-tens)
({:min 0.0,
  :mean 97.47846137154495,
  :ecount 147456,
  :standard-deviation 60.735870710272195,
  :median 97.0,
  :max 248.0}
 {:min 5.0,
  :mean 101.00441487631271,
  :ecount 147456,
  :standard-deviation 58.55138215866227,
  :median 102.0,
  :max 255.0}
 {:min 5.0,
  :mean 113.43905978729688,
  :ecount 147456,
  :standard-deviation 57.79730239195752,
  :median 115.0,
  :max 255.0})
```

### Cropping/Resizing Images

For simple resize operations, we provide a resize convenience function that uses the
buffered image graphics canvas to render a resized image into another image.

Cropping can be done 2 ways.  The draw-image! method can crop or you can select
regions of the images using the tensor api and then us the datatype copy! operation.

```clojure
;;Drawing images work well in order to copy parts of one:
(def new-img (bufimg/new-image 512 512 :int-argb))
#'user/new-img
user> (bufimg/draw-image! test-img new-img :dst-y-offset 128)
#object[java.awt.image.BufferedImage 0x1ec0f94c "BufferedImage@1ec0f94c: type = 2 DirectColorModel: rmask=ff0000 gmask=ff00 bmask=ff amask=ff000000 IntegerInterleavedRaster: width = 512 height = 512 #Bands = 4 xOff = 0 yOff = 0 dataOffset[0] 0"]

;;But so does selecting subrects of tensors

user> (def copy-tens (dtt/select (bufimg/as-ubyte-tensor new-img)
                                 (range 128 (+ 288 128)) :all [0 1 2]))
#'user/copy-tens
user> copy-tens
#tech.v2.tensor<uint8>[288 512 3]
[[[172 170 170]
  [172 170 170]
  [171 169 169]
  ...
  [ 24  18  23]
  [ 24  18  23]
  [ 24  18  23]]


user> (dtype/copy! test-img copy-tens)
#tech.v2.tensor<uint8>[288 512 3]
[[[172 170 170]
  [172 170 170]
  [171 169 169]
  ...
  [ 24  18  23]
  [ 24  18  23]
  [ 24  18  23]]
  ...
```


### API Reference



* `load` - load an image.  clojure.java.io/input-stream is called on the fname.
* `new-image` - Create a new image.  Arguments are in row-major format:
   height,width,img-type.
* `image-type` - Returns the image type of the image.
* `image-channel-map` - Returns a map of the channel names to indexes when the image
   is interpreted as a uint8 tensor.
* `as-ubyte-tensor` - interpret image as a uint8 tensor.  Works for byte and int image    types.
* `draw-image!` - Draw a source image onto a dest image.  You can specify the
   source/dest rectangles and an interpolation method when resizing.
* `downsample-bilinear` - Convenience method around `draw-image`.  Default is to halve
  the height and width of the image.
* `resize` - Convenience method that auto-determines the interpolation type based on
  source/dest width ratio.
* `clone` - Clone an image.  Simply calls `tech.v2.datatype/clone`.
* `save!` - save the image.  You can specify a format optionally aside from the fname
   or it will be inferred from the part of the fname following the last '.'.


It is important to note that all of the datatype methods work on the image - `ecount`,
`shape`, `get-datatype`, `copy!`, `set-constant!`.  All of the tensor methods work on
the image - `ensure-tensor`, `select`, `reshape`, `transpose`.  You can create a
tensor reader or writer from the image to get fully typed access to it in
height,width,channel as demonstrated above.  Finally there is a new container type,
`:buffered-image`, so you can use the `tech.v2.datatype/make-container` with
container-type `:buffered-image` a datatype (which can only be `:uint8) and a shape
to create a new image:

```clojure
tech.libs.buffered-image-test> (dtype/make-container :buffered-image :uint8 [4 4 4])
#object[java.awt.image.BufferedImage 0x67005e49 "BufferedImage@67005e49: type = 6 ColorModel: #pixelBits = 32 numComponents = 4 color space = java.awt.color.ICC_ColorSpace@302cd36f transparency = 3 has alpha = true isAlphaPre = false ByteInterleavedRaster: width = 4 height = 4 #numDataElements 4 dataOff[0] = 3"]
```
