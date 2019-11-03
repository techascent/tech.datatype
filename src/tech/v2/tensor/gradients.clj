(ns tech.v2.tensor.gradients
  "https://reference.wolfram.com/language/guide/ColorSchemes.html"
  (:require [clojure.java.io :as io]
            [tech.v2.tensor :as dtt]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dfn]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.tensor.typecast :as tens-typecast]
            [tech.v2.datatype.typecast :as typecast]
            [clojure.edn :as edn]
            [tech.parallel.for :as pfor]
            [tech.libs.buffered-image :as bufimg]
            [clojure.tools.logging :as log])
  (:import [java.awt.image BufferedImage]
           [tech.v2.tensor ByteTensorReader]
           [clojure.lang IFn]))


(def gradient-map (delay (-> (io/resource "gradients.edn")
                             (slurp)
                             (edn/read-string))))
(def gradient-tens (delay (-> (io/resource "gradients.png")
                              (bufimg/load)
                              (dtt/ensure-tensor))))



(defn- flp-close
  [val desired & [error]]
  (< (Math/abs (- (double val) (double desired)))
     (double (or error 0.001))))


(defn apply-gradient
  "Apply a gradient to a tensor returning an image.  If data-min, data-max aren't
  provided they are found in the data.  A buffered image is returned.
  src-tens - Source tensor whose shape determines the shape of the final image.
  gradient-name -  may be a keyword, in which it must be a key in @gradient-map and
      these gradients come from:
      https://reference.wolfram.com/language/guide/ColorSchemes.html.
    gradient-name may be a tensor of dimensions [n 3].
    gradient-name may be a function that takes a value from 0-1 and returns a tuple
    of length 3.

  Additional arguments:
  :data-min :data-max - If provided then the data isn't scanned for min and max.  If min
    is equal to 0 and max is equal to 1.0 then the data doesn't need to be normalized.
    data ranges are clamped to min and max.
  :alpha? - If true, an image with an alpha channel is returned.  This is useful for
    when your data has NAN or INFs as in that case the returned image is transparent
    in those sections.
  :check-invalid? - If true then the data is scanned for NAN or INF's.  Used in
    conjunction with :alpha?
  :invert-gradient? - When true, reverses the provided gradient."
  [src-tens gradient-name & {:keys [data-min data-max
                                    alpha?
                                    check-invalid?
                                    invert-gradient?
                                    gradient-default-n]
                             :or {gradient-default-n 200}}]
  (let [img-shape (dtype/shape src-tens)
        n-pixels (dtype/ecount src-tens)
        valid-indexes (when check-invalid?
                        (dfn/argfilter dfn/valid?
                                       (dtype/->reader src-tens
                                                       :float64)))
        valid-indexes (if-not (= (dtype/ecount valid-indexes) n-pixels)
                        valid-indexes
                        nil)
        _ (when (and valid-indexes (not alpha?))
            (log/warnf "Invalid data valids detected but alpha not specified.
This leads to ambiguous results as pixels not written to will be black but not transparent."))
        src-reader (if valid-indexes
                     (dtype/indexed-reader valid-indexes src-tens)
                     src-tens)
        {data-min :min
         data-max :max} (if (and data-min data-max)
                          {:min data-min
                           :max data-max}
                          (dfn/descriptive-stats src-reader [:min :max]))
        data-min (double data-min)
        data-max (double data-max)
        _ (when (or (dfn/invalid? data-min)
                    (dfn/invalid? data-max))
            (throw (Exception. "NAN or INF in src data detected!")))
        data-range (- data-max data-min)
        src-reader (if-not (and (flp-close 0.0 data-min)
                                (flp-close 1.0 data-max))
                     (unary-op/unary-reader :float64
                                            (-> (- x data-min)
                                                (/ data-range))
                                            src-reader)
                     src-reader)
        src-reader (if invert-gradient?
                     (unary-op/unary-reader :float64
                                            (- 1.0 x)
                                            src-reader)
                     src-reader)
        src-reader (typecast/datatype->reader :float64 src-reader)
        img-type (if alpha?
                   :byte-abgr
                   :byte-bgr)
        res-image (case (count img-shape)
                    2 (bufimg/new-image (second img-shape) (first img-shape) img-type)
                    1 (bufimg/new-image (first img-shape) 1 img-type))
        ;;Flatten out src-tens and res-tens and make them readers
        n-channels (long (if alpha? 4 3))
        res-tens (dtt/reshape res-image [n-pixels n-channels])
        res-tens (if valid-indexes
                   (dtt/select res-tens valid-indexes (range n-channels))
                   res-tens)
        res-tens (tens-typecast/datatype->tensor-writer
                  :uint8 res-tens)
        n-gradient-increments (long gradient-default-n)
        gradient-line
        (cond
          (keyword? gradient-name)
          (let [src-gradient-info (get @gradient-map gradient-name)
                _ (when-not src-gradient-info
                    (throw (Exception.
                            (format "Failed to find gradient %s"
                                    gradient-name))))
                gradient-tens @gradient-tens]
            (dtt/select gradient-tens
                        (:tensor-index src-gradient-info)
                        :all :all))
          (dtt/tensor? gradient-name)
          gradient-name
          (instance? IFn gradient-name)
          (dtt/->tensor
           (->> (range n-gradient-increments)
                (map (fn [idx]
                       (let [p-val (/ (double idx)
                                      (double n-gradient-increments))
                             grad-val (gradient-name p-val)]
                         (when-not (= 3 (count grad-val))
                           (throw (Exception. (format
                                               "Gradient fns must return bgr tuples:
function returned: %s"
                                               grad-val))))
                         grad-val))))))
        n-gradient-increments (long (first (dtype/shape gradient-line)))
        gradient-line (tens-typecast/datatype->tensor-reader
                       :uint8
                       gradient-line)
        line-last-idx (double (dec n-gradient-increments))
        n-pixels (long (if valid-indexes
                         (dtype/ecount valid-indexes)
                         n-pixels))]
    (if alpha?
      (pfor/parallel-for
       idx
       n-pixels
       (let [p-value (min 1.0 (max 0.0 (.read src-reader idx)))
             line-idx (long (Math/round (* p-value line-last-idx)))]
         ;;alpha channel first
         (.write2d res-tens idx 0 255)
         (.write2d res-tens idx 1 (.read2d gradient-line line-idx 0))
         (.write2d res-tens idx 2 (.read2d gradient-line line-idx 1))
         (.write2d res-tens idx 3 (.read2d gradient-line line-idx 2))))
      (pfor/parallel-for
       idx
       n-pixels
       (let [p-value (min 1.0 (max 0.0 (.read src-reader idx)))
             line-idx (long (Math/round (* p-value line-last-idx)))]
         (.write2d res-tens idx 0 (.read2d gradient-line line-idx 0))
         (.write2d res-tens idx 1 (.read2d gradient-line line-idx 1))
         (.write2d res-tens idx 2 (.read2d gradient-line line-idx 2)))))
    res-image))


(comment
  (require '[clojure.java.io :as io])
  (io/make-parents "gradient-demo/test.txt")

  (def test-src-tens (dtt/->tensor (repeat 128 (range 0 512))))
  (doseq [grad-name (keys @gradient-map)]
    (bufimg/save! (apply-gradient test-src-tens grad-name)
                  "PNG"
                  (format "gradient-demo/%s.png" (name grad-name))))
  (defn bad-range
    [start end]
    (->> (range start end)
         (map (fn [item]
                (if (> (rand) 0.5)
                  Double/NaN
                  item)))))
  ;;Sometimes data has NAN's or INF's
  (def test-nan-tens (dtt/->tensor (repeatedly 128 #(bad-range 0 512))))
  (doseq [grad-name (keys @gradient-map)]
    (bufimg/save! (apply-gradient test-nan-tens grad-name
                                  :alpha? true
                                  :check-invalid? true)
                  "PNG"
                  (format "gradient-demo/%s-nan.png" (name grad-name))))

  (def custom-gradient-tens (dtt/->tensor
                             (->> (range 100)
                                  (map (fn [idx]
                                         (let [p-value (/ (double idx)
                                                          (double 100))]
                                           [(* 255 p-value) 0 (* (- 1.0 p-value)
                                                                 255)]))))))

  (bufimg/save! (apply-gradient test-src-tens custom-gradient-tens)
                "PNG"
                "gradient-demo/custom-tensor-gradient.png")

  (defn custom-gradient-fn
    [^double p-value]
    (let [one-m-p (- 1.0 p-value)]
      [(* 255 one-m-p) (* 255 p-value) (* 255 one-m-p)]))

  (bufimg/save! (apply-gradient test-src-tens custom-gradient-fn)
                "PNG"
                "gradient-demo/custom-ifn-gradient.png")
  )
