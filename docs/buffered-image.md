# Buffered Image Support


`tech.datatype` contains support for loading/saving buffered images
and creating tensors from buffered images.


## Usage

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
