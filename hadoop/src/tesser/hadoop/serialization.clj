(ns tesser.hadoop.serialization
  "Lets us serialize Tesser reduction state for transport through various
  distributed systems"
  (:require [clojure.data.fressian :as fress]
            [clojure.walk :as walk]
            [parkour.wrapper :as wrapper]
            [tesser.quantiles :as quantiles])
  (:import (tesser.hadoop_support FressianWritable)
           (java.io DataInput
                    DataOutput
                    ByteArrayInputStream
                    ByteArrayOutputStream)
           (java.nio ByteBuffer)
           (org.fressian.handlers WriteHandler
                                  ReadHandler)
           (org.HdrHistogram DoubleHistogram)
           (tesser.quantiles DualHistogram)
           (com.clearspring.analytics.stream.cardinality
                HyperLogLogPlus
                HyperLogLogPlus$Builder)))

; TODO: extract serialization for math into tesser.math itself? Common
; interface somewhere?

(defmacro handler
  "Takes a classname as a symbol, a tag name as a string, and bodies for write
  and read functions. Provides a special syntax for writing the component
  count: (write-tag! some-number), which expands to (.writeTag writer tag
  some-number). Returns a map with two keys: :readers, and :writers, each value
  being a map suitable for use as a Fressian reader or writer, respectively.

      (handler QDigest \"q-digest\"
        (write [_ writer digest]
          (write-tag! 1)
          (.writeBytes writer (QDigest/serialize digest)))
        (read [_ reader tag component-count]
          (QDigest/deserialize ^bytes (.readObject reader))))"
  [classname tag write-expr read-expr]
  (let [writer-sym  (-> write-expr second second)
        write-expr (walk/prewalk
                     (fn [form]
                       (if (and (list? form)
                                (= 'write-tag! (first form)))
                         (let [count-expr (second form)]
                           (assert
                             (= 2 (count form))
                             "write-tag! takes 1 argument: a component count.")
                           `(.writeTag ~writer-sym ~tag ~count-expr))
                         form))
                     write-expr)]
    `{:writers {~classname {~tag (reify WriteHandler ~write-expr)}}
      :readers {~tag (reify ReadHandler ~read-expr)}}))

(defmacro handlers
  "Takes a flat series of handler quartets: class-name, tag, writer, reader, as
  per `handler`. Returns a `{:writers {...}, :readers {...}}` map, where all
  writers are merged into a unified map, merged with the clojure default
  handlers, and wrapped with inheritance/associative lookups. Does the same for
  the readers map, but without inheritance lookups. `:readers` and `:writers`
  may be passed to Fressian.

      (handlers
        QDigest \"q-digest\"
        (write [_ writer digest]
          (write-tag! 1)
          (.writeBytes writer (QDigest/serialize digest)))
        (read [_ reader tag component-count]
          (QDigest/deserialize ^bytes (.readObject reader)))

        clojure.lang.PersistentVector \"vector\"
        (write [_ writer v]
          (write-tag! (count v))
          (doseq [e v]
            (.writeObject writer e)))
        (read [_ rdr tag component-count]
              (let [v (transient [])]
                (dotimes [_ component-count]
                  (conj! v (.readObject rdr)))
                (persistent! v))))"
  [& quartets]
  (let [handlers (partition 4 quartets)
        names    (repeatedly (count handlers) (partial gensym "handler"))]
    ; Bind each handler to a symbol
    `(let [~@(->> handlers
               (map (partial cons `handler))
               (interleave names))
           ; Wrap up handlers into a vector
           handlers#   [~@names]
           ; Extract writers and readers
           writers# (map :writers handlers#)
           readers# (map :readers handlers#)]
       ; Merge writers/readers together into unified maps
       {:writers (->> writers#
                   (cons fress/clojure-write-handlers)
                   (reduce merge)
                   fress/associative-lookup
                   fress/inheritance-lookup)
        :readers (->> readers#
                   (cons fress/clojure-read-handlers)
                   (reduce merge)
                   fress/associative-lookup)})))

(defn ^ByteBuffer rewind
  "Rewind a byte buffer, returning it."
  [^ByteBuffer buf]
  (doto buf (.rewind)))

(def fress-handlers
  "All the serialization you could ever need in one fat monolithic, hmm, I
  dunno, is this really such a good idea?"
  (handlers
    DualHistogram "tesser.digest/dual"
    (write [_ writer digest]
           (write-tag! 2)
           (fress/write-object writer (:neg digest))
           (fress/write-object writer (:pos digest)))

    (read [_ reader tag component-count]
          (DualHistogram. (.readObject reader)
                          (.readObject reader)))


    DoubleHistogram "org.HdrHistogram.DoubleHistogram"
    (write [_ writer digest]
           (write-tag! 1)
           (let [buf (ByteBuffer/allocate
                       (.getNeededByteBufferCapacity digest))]
             (.encodeIntoCompressedByteBuffer digest buf)
             (->> buf
                  rewind
                  quantiles/byte-buffer->bytes
                  (.writeBytes writer))))

    (read [_ reader tag component-count]
          (-> (quantiles/bytes->byte-buffer ^bytes (.readObject reader))
              (DoubleHistogram/decodeFromCompressedByteBuffer 0)))


    clojure.lang.ISeq "seq"
    (write [_ w l]
           (write-tag! 2)
           (.writeInt w (count l))
           (doseq [e l]
             (fress/write-object w e)))

    (read [_ rdr tag component-count]
          (let [c   (.readInt rdr)]
            ; Empty seqs eval to nil, which breaks '() roundtrip equality.
            (if (zero? c)
              '()
              (let [ary (object-array c)]
                (dotimes [i c]
                  (aset ary i (.readObject rdr)))
                (seq ary)))))


    clojure.lang.PersistentVector "vector"
    (write [_ writer v]
           (write-tag! 2)
           (.writeInt writer (count v))
           (doseq [e v]
             (fress/write-object writer e)))

    (read [_ rdr tag component-count]
          (loop [i (.readInt rdr)
                 v (transient [])]
            (if (pos? i)
              (recur (dec i)
                     (conj! v (.readObject rdr)))
              (persistent! v))))

    clojure.lang.IPersistentMap "persistent-map"
    (write [_ w m]
           (write-tag! 2)
           (.writeInt w (count m))
           (doseq [[k v] m]
             (.writeObject w k)
             (.writeObject w v)))

    (read [_ rdr tag component-count]
            (loop [i (.readInt rdr)
                   m (transient {})]
              (if (pos? i)
                (recur (dec i)
                       (assoc! m (.readObject rdr) (.readObject rdr)))
                (persistent! m))))


    clojure.lang.PersistentTreeMap "sorted-map"
    (write [_ w m]
           (write-tag! 2)
           (.writeInt w (count m))
           (doseq [[k v] m]
             (.writeObject w k)
             (.writeObject w v)))

    (read [_ rdr tag component-count]
          (loop [i (.readInt rdr)
                 m (sorted-map)]
            (if (pos? i)
              (recur (dec i)
                     (assoc m (.readObject rdr) (.readObject rdr)))
              m)))


    clojure.lang.IPersistentSet "persistent-set"
    (write [_ w set]
           (write-tag! 2)
           (.writeInt w (count set))
           (doseq [e set]
             (.writeObject w e)))

    (read [_ rdr tag component-count]
          (loop [i (.readInt rdr)
                 s (transient (hash-set))]
            (if (pos? i)
              (recur (dec i)
                     (conj! s (.readObject rdr)))
              (persistent! s))))


    clojure.lang.PersistentTreeSet "sorted-set"
    (write [_ w set]
           (write-tag! 2)
           (.writeInt w (count set))
           (doseq [e set]
             (.writeObject w e)))

    (read [_ rdr tag component-count]
          (loop [i (.readInt rdr)
                 s (sorted-set)]
            (if (pos? i)
              (recur (dec i)
                     (conj s (.readObject rdr)))
              s)))


    HyperLogLogPlus "hyperloglogplus-estimator"
    (write [_ w hll]
           (write-tag! 1)
           (.writeBytes w (.getBytes ^HyperLogLogPlus hll)))

    (read [_ rdr _ _]
          (HyperLogLogPlus$Builder/build ^bytes (.readObject rdr)))))

(defn ^bytes write-byte-array
  "Dump a structure to a byte array using our handlers."
  [x]
  (let [out    (ByteArrayOutputStream.)
        writer (fress/create-writer out :handlers (:writers fress-handlers))]
    (fress/write-object writer x)
    (.toByteArray out)))

(defn read-byte-array
  "Parse a byte array and return a single object, using our handlers."
  [^bytes bs]
  (let [in     (ByteArrayInputStream. bs)
        reader (fress/create-reader in :handlers (:readers fress-handlers))]
    (fress/read-object reader)))

; Serializes values to and from Fressian records, delimited by a 32-bit int
; length header. Lord have mercy on my soul.
(set! FressianWritable/readFieldsFn
      (fn read-fields [^FressianWritable w ^DataInput in]
        (let [buffer (-> in
                       .readInt
                       byte-array)]
          ; Copy input to buffer
          (.readFully in buffer)
          (set! (.state w)
                (-> buffer
                  (ByteArrayInputStream.)
                  (fress/create-reader :handlers (:readers fress-handlers))
                  (fress/read-object))))))

(set! FressianWritable/writeFn
      (fn write [^FressianWritable w ^DataOutput out]
        (let [value  (.state w)
              buf    (ByteArrayOutputStream.)
              writer (fress/create-writer
                       buf :handlers (:writers fress-handlers))
              _      (fress/write-object writer value)]
          (.writeInt out (.size buf))
          (.write out (.toByteArray buf)))))

; Tell Parkour how to clobber/extract values from our Fressian-backed
; FressianWritable
(extend-protocol wrapper/Wrapper
  FressianWritable
  (unwrap [this]
          (.state this))
  (rewrap [this obj]
          (set! (.state this) obj)
          this))
