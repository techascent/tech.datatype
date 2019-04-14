(ns tech.libs.blas
  (:require [tech.jna :as jna]
            [tech.datatype.typecast :refer [ensure-ptr-like]]))


(def ^:dynamic *system-blas-lib-name* "blas")


(defn ^:private system-blas-lib
  []
  (try
    (jna/load-library *system-blas-lib-name*)
    (catch Throwable e
      (println "Failed to load native blas:" (.getMessage e))
      nil)))

(defn has-blas?
  []
  (boolean (system-blas-lib)))


(defn openblas?
  []
  (boolean
   (when-let [system-blas (system-blas-lib)]
     (jna/find-function "openblas_get_num_threads" *system-blas-lib-name*))))


(jna/def-jna-fn *system-blas-lib-name* openblas_get_num_threads
  "Get number of openblas threads"
  Integer)


(jna/def-jna-fn *system-blas-lib-name* openblas_set_num_threads
  "Set number of openblas threads"
  nil
  [num-threads int])


;; typedef enum CBLAS_ORDER     {CblasRowMajor=101, CblasColMajor=102} CBLAS_ORDER;
;; typedef enum CBLAS_TRANSPOSE {CblasNoTrans=111, CblasTrans=112, CblasConjTrans=113, CblasConjNoTrans=114} CBLAS_TRANSPOSE;
;; typedef enum CBLAS_UPLO      {CblasUpper=121, CblasLower=122} CBLAS_UPLO;
;; typedef enum CBLAS_DIAG      {CblasNonUnit=131, CblasUnit=132} CBLAS_DIAG;
;; typedef enum CBLAS_SIDE      {CblasLeft=141, CblasRight=142} CBLAS_SIDE;

(def enums
  {:row-major 101 :column-major 102
   :no-transpose 111 :transpose 112 :conjugate-transpose 113 :conjugate-no-transpose 114
   :upper 121 :lower 122
   :non-unit 131 :unit 132
   :left 141 :right 142})

(defn enum-value
  [enum-name]
  (int
   (if-let [retval (get enums enum-name)]
     retval
     (throw (ex-info "Failed to find enum:"
                     {:enum-name enum-name})))))

;; void cblas_sgemm(OPENBLAS_CONST enum CBLAS_ORDER Order,
;;                  OPENBLAS_CONST enum CBLAS_TRANSPOSE TransA,
;;                  OPENBLAS_CONST enum CBLAS_TRANSPOSE TransB,
;;                  OPENBLAS_CONST blasint M,
;;                  OPENBLAS_CONST blasint N,
;;                  OPENBLAS_CONST blasint K,
;; 		    OPENBLAS_CONST float alpha,
;;                  OPENBLAS_CONST float *A,
;;                  OPENBLAS_CONST blasint lda,
;;                  OPENBLAS_CONST float *B,
;;                  OPENBLAS_CONST blasint ldb,
;;                  OPENBLAS_CONST float beta,
;;                  float *C, OPENBLAS_CONST blasint ldc);


(defn- ensure-blas
  []
  (when-not (has-blas?)
    (throw (ex-info "System blas is unavailable." {}))))


(defn bool->blas-transpose
  [trans?]
  (int
   (enum-value (if trans? :transpose :no-transpose))))


(jna/def-jna-fn *system-blas-lib-name* cblas_sgemm
  "float32 gemm"
  nil
  [order enum-value]
  [trans-a? bool->blas-transpose]
  [trans-b? bool->blas-transpose]
  [M int]
  [N int]
  [K int]
  [alpha float]
  [A ensure-ptr-like]
  [lda int]
  [B ensure-ptr-like]
  [ldb int]
  [beta float]
  [C ensure-ptr-like]
  [ldc int])


(jna/def-jna-fn *system-blas-lib-name* cblas_dgemm
  "float64 gemm"
  nil
  [order enum-value]
  [trans-a? bool->blas-transpose]
  [trans-b? bool->blas-transpose]
  [M int]
  [N int]
  [K int]
  [alpha double]
  [A ensure-ptr-like]
  [lda int]
  [B ensure-ptr-like]
  [ldb int]
  [beta double]
  [C ensure-ptr-like]
  [ldc int])


(jna/def-jna-fn *system-blas-lib-name* cblas_sgemv
  "float32 gemv"
  nil
  [order enum-value]
  [trans-a? bool->blas-transpose]
  [M int]
  [N int]
  [alpha float]
  [A ensure-ptr-like]
  [lda int]
  [x ensure-ptr-like]
  [incx int]
  [beta float]
  [y ensure-ptr-like]
  [incy int])


(jna/def-jna-fn *system-blas-lib-name* cblas_dgemv
  "float64 gemv"
  nil
  [order enum-value]
  [trans-a? bool->blas-transpose]
  [M int]
  [N int]
  [alpha double]
  [A ensure-ptr-like]
  [lda int]
  [x ensure-ptr-like]
  [incx int]
  [beta double]
  [y ensure-ptr-like]
  [incy int])
