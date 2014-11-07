(defpackage woo.bbi-wrap
  (:nicknames :bbi-wrap)
  (:use :cl :trivial-gray-streams)
  (:import-from :alexandria
	   #:ensure-list)
  (:import-from :basic-binary-ipc
		#:connection-available-p
		#:data-available-p
		#:remote-disconnected-p
		#:ready-to-write-p)
  (:export
   #:socket-data
   #:socket-sock
   #:socket-outputs
   #:socket-output-pos
   #:socket-events
   #:write-socket-data
   #:send-some-data
   #:new-connection
   #:bbi-output-stream
   #:tcp-loop
   ))

(in-package :woo.bbi-wrap)

(defstruct server sock clients)
(defstruct socket data sock outputs output-pos events server)

(defun write-socket-data (client data &key write-cb)
  (let ((data (etypecase data
		(string (trivial-utf-8:string-to-utf-8-bytes data))
		((vector (unsigned-byte 8)) data))))
    (setf (socket-outputs client)
	  (append (socket-outputs client) (list (cons data write-cb))))
    (pushnew 'basic-binary-ipc:ready-to-write-p (socket-events client))))

(defun send-some-data (client)
  (if (not (socket-outputs client))
      (setf (socket-events client)
	    (delete 'ready-to-write-p (socket-events client)))
      (loop
	 for bytes = (basic-binary-ipc:write-to-stream
		      (socket-sock client)
		      (caar (socket-outputs client))
		      :start (socket-output-pos client))
	 until (= bytes 0)
	 do (incf (socket-output-pos client) bytes)
	 when (>= (socket-output-pos client) (length (caar (socket-outputs client))))
	 do
	   (when (cdar (socket-outputs client))
	     (funcall (cdar (socket-outputs client)) client))
	   (pop (socket-outputs client))
	   (setf (socket-output-pos client) 0)
	 unless (socket-outputs client)
	 do (setf (socket-events client)
		  (delete 'ready-to-write-p (socket-events client)))
	   (return))))

(defun new-connection (server)
  (let ((socket (basic-binary-ipc:accept-connection (server-sock server))))
    (when socket
      (make-socket :data nil :sock socket :outputs nil :output-pos 0 :events (list 'data-available-p 'remote-disconnected-p)
		   :server server))))

(defun close-socket (socket)
  (setf (server-clients (socket-server socket))
	(delete socket (server-clients (socket-server socket))))
  (basic-binary-ipc:close-socket (socket-sock socket)))

(defun tcp-loop (address port read-cb event-cb connect-cb post-init-cb
		     &optional (bufsize (* 1024 1024)))
  (basic-binary-ipc:with-socket
      (server (basic-binary-ipc:make-ipv4-tcp-server (basic-binary-ipc:resolve-ipv4-address address) port))
    (funcall post-init-cb)
    (let ((srv (make-server :sock server :clients nil))
	  (buffer (make-array bufsize :element-type '(unsigned-byte 8))))
      (symbol-macrolet ((clients (server-clients srv)))
	(flet ((client-work (client events)
		 (when events
		   (let ((events (ensure-list events)))
		     (cond
		       ((member 'remote-disconnected-p events)
			(funcall event-cb events)
			(close-socket client))
		       (t
			(progn
			  (when (member 'data-available-p events)
			    (loop for bytes = (basic-binary-ipc:read-from-stream
					       (socket-sock client) buffer)
			       when (> bytes 0) do (funcall read-cb client (subseq buffer 0 bytes))
			       while (= bytes bufsize)))
			  (when (member 'ready-to-write-p events)
			    (send-some-data client)))))))))
	  (loop
	     for (server-e . clients-e) =
	       (basic-binary-ipc:poll-sockets (cons server (mapcar #'socket-sock clients))
					      (cons 'connection-available-p
						    (mapcar #'socket-events clients))
					      :indefinite)
	     when server-e do (push (new-connection srv) clients)
	       (funcall connect-cb (car clients))
	     do (mapc #'client-work clients clients-e)))))))

;Streams

(defclass bbi-stream (trivial-gray-stream-mixin)
  ((socket :accessor stream-socket :initarg :socket :initform nil))
  (:documentation "The underlying class for async streams. Wraps a bbi socket class."))
(defclass bbi-output-stream (bbi-stream fundamental-binary-output-stream) ()
  (:documentation "Async output stream."))


;; -----------------------------------------------------------------------------
;; base stream
;; -----------------------------------------------------------------------------
(defmethod stream-output-type ((stream bbi-stream))
  "This is always a binary stream."
  '(unsigned-byte 8))

(defmethod stream-element-type ((stream bbi-stream))
  "This is always a binary stream."
  '(unsigned-byte 8))

(defmethod open-stream-p ((stream bbi-stream))
  "Test the underlying socket to see if this stream is open."
  (let ((socket (socket-sock (stream-socket stream))))
    (not (basic-binary-ipc:socket-closed-p socket))))

(defmethod close ((stream bbi-stream) &key abort)
  "Close the stream. If aborting, attempt to clear out remaining data in the
   buffers before closing (is this really needed?)"
  (when abort
    (when (output-stream-p stream)
      (clear-output stream))
    (when (input-stream-p stream)
      (clear-input stream)))
  (basic-binary-ipc:close-socket (socket-sock (stream-socket stream))))

;; -----------------------------------------------------------------------------
;; output stream
;; -----------------------------------------------------------------------------
(defmethod stream-clear-output ((stream bbi-output-stream))
  "Attempt to clear the output buffer of an output stream."
  (when (open-stream-p stream)
    (let* ((socket (stream-socket stream)))
      (setf (socket-outputs socket) nil
	    (socket-output-pos socket) nil))))

(defmethod stream-force-output ((stream bbi-output-stream))
  "Force an output stream to send its data to the underlying fd.")

(defmethod stream-finish-output ((stream bbi-output-stream))
  "Really, since we're async, same as force-output."
  (stream-force-output stream))

(defmethod stream-write-byte ((stream bbi-output-stream) byte)
  "Write one byte to the underlying socket."
  (when (open-stream-p stream)
    (write-socket-data (stream-socket stream) (make-array 1 :element-type '(unsigned-byte 8)
							      :initial-element byte))))
  
(defmethod stream-write-sequence ((stream bbi-output-stream) sequence start end &key)
  "Write a sequence of bytes to the underlying socket."
  (when (open-stream-p stream)
    (cond
      ((and (= start 0) (= end (length sequence))
	    (typep sequence '(vector (unsigned-byte 8))))
       (write-socket-data (stream-socket stream) sequence))
      ((vectorp sequence)
       (let ((seq (subseq sequence start end)))
	 (write-socket-data (stream-socket stream) seq)))
      (t
       (let ((seq (make-array (- end start) :element-type '(unsigned-byte 8)
			      :initial-contents (subseq sequence start end))))
	 (write-socket-data (stream-socket stream) seq))))))
