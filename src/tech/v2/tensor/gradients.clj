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
  (:import [java.awt.image BufferedImage]))


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
  "Apply a gradient to a tensor returning a new tensor.  If data-min, data-max aren't
  provided they are found in the data.  A buffered image is returned.
  gradient-names are (keys gradient-map).  Gradients themselves are taken from
  https://reference.wolfram.com/language/guide/ColorSchemes.html"
  [src-tens gradient-name & {:keys [data-min data-max
                                    alpha?
                                    check-invalid?
                                    invert-gradient?]}]
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
        src-gradient-info (get @gradient-map gradient-name)
        _ (when-not src-gradient-info
            (throw (Exception. (format "Failed to find gradient %s"
                                       gradient-name))))
        gradient-tens @gradient-tens
        gradient-line (tens-typecast/datatype->tensor-reader
                       :uint8
                       (dtt/select gradient-tens
                                   (:tensor-index src-gradient-info)
                                   :all :all))
        line-last-idx (double (dec (long (second (dtype/shape gradient-tens)))))
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
                  (format "gradient-demo/%s-nan.png" (name grad-name)))))
