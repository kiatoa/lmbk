;; Copyright (C) 2008, Matthew Welland
;;

(require-extension sqlite3 regex posix srfi-1 srfi-4)

(include "../sclibs/args.scm")

(define help "
Usage: lmbk [options] 

GENERAL
  -h                      : this help
  -debug                  : debug mode

QUERIES
  -lsbkups                : list areas registered for backup
  -find fpath             : list files in backup repository
  -lookup md5sum_patt     : lookup file by md5sum pattern

BACKUP
  -b path_or_file         : mark files for backup
  -r                      : recursive
  -p                      : prep data to be backed up
  -c                      : create iso (NOT IMPLEMENTED YET)
  -cont                   : continue with backup in STAGING_DIR

SETUP
  -s key val              : set setup value for key to val
                            without val it deletes the key
       Important keys:
         STAGING_DIR      : path where the data to be backed up is staged    

MAINT
  -i path                 : import contents of backup disk at path

NOTES

")

;; process

(define remargs (get-args (argv)
			  (list "-b"          "-s"   
				"-find"       "-lookup")
			  
			  (list "-h"          "-debug"
				"-r"          "-c"
				"-p"          "-lsbkups"
				"-cont"        )
			  arg-hash
			  0))

(define dbhome (conc (getenv "HOME") "/.lmbk"))
(if (not (directory? dbhome))
    (create-directory dbhome))
(define dbpath (conc dbhome "/lmbk.db"))

(define (mk-bkdb-tables db)
  (sqlite3:exec db "CREATE TABLE bkpaths(id INTEGER PRIMARY KEY,host TEXT,path_id INTEGER,recursive INTEGER,lastdate INTEGER);")
  ;; filt_type; 0=must be false, 1=must be true
  (sqlite3:exec db "CREATE TABLE filters(id INTEGER PRIMARY KEY,bkpath_id INTEGER,filter TEXT,filt_type INTEGER);")
  (sqlite3:exec db "CREATE TABLE data   (id INTEGER PRIMARY KEY,md5sum TEXT,size INTEGER,ftype_id INTEGER);")
  ;; a "file" is a unique combination of data, perms, and path NB// in future make paths be host://path/ i.e. make host part of path
  (sqlite3:exec db "CREATE TABLE files  (id INTEGER PRIMARY KEY,path_id INTEGER,name TEXT,perms INTEGER,owner_id INTEGER,group_id INTEGER,ftype_id INTEGER,date_added INTEGER);")
  (sqlite3:exec db "CREATE TABLE dirs   (id INTEGER PRIMARY KEY,name TEXT,parent_id INTEGER,perms INTEGER,owner INTEGER);")
  (sqlite3:exec db "CREATE TABLE meta   (id INTEGER PRIMARY KEY,key TEXT,val TEXT);")
  ;; type_id; 0=CDRW.650Gig 1=CDR.650Gig, 2=DVDR4.7Gig, 3=DVDRAM
  (sqlite3:exec db "CREATE TABLE media  (id INTEGER PRIMARY KEY,name TEXT,date_created INTEGER,type_id INTEGER,num_writes INTEGER);")
  ;; rewriteable; 0=no, 1=yes
  (sqlite3:exec db "CREATE TABLE mtypes (id INTEGER PRIMARY KEY,mtype TEXT,size INTEGER,code TEXT,rewriteable INTEGER);")
  ;; compressed; 0=not compressed, 1=gzip, 2=bzip
  (sqlite3:exec db "CREATE TABLE bkups  (id INTEGER PRIMARY KEY,file_id INTEGER,data_id INTEGER,media_id INTEGER,compressed INTEGER,bkup_date INTEGER);")
  (sqlite3:exec db "CREATE TABLE ftypes (id INTEGER PRIMARY KEY,ftype TEXT,compressible INTEGER);")
  (map (lambda (m s c r)
         (sqlite3:exec db "INSERT INTO mtypes (mtype,size,code,rewriteable) VALUES(?,?,?,?);" m s c r))
       '("CDRW"     "CDRW"      "CDR"     "CDR"    "DVDR"    "DVDRAM")
       '(0.65        0.700      0.65      0.700     4.7       4.7)
       '("CDRW0.65" "CDRW0.70" "CDR0.65"  "CDR0.70" "DVDR4.7" "DVDRAM4.7")
       '(1           1          0         0         0         1 )))

(define (lmbk:mk-db tblmkr dbpath)
  (let ((havedb (file-exists? dbpath))
	(db     (sqlite3:open dbpath)))
    (sqlite3:set-busy-timeout! db 1000000)
    (if (not havedb)
	(begin
	  (print "INFO: Creating new lmbk db at " dbpath)
	  (tblmkr db)))
    (sqlite3:exec db "PRAGMA synchronous = OFF;")
    db))

;;======================================================================
;; Bkupdat - record for all the "stuff" in a backup run
;;======================================================================

(define-inline (bkup:get-db         dat)(vector-ref dat 0))
(define-inline (bkup:get-media-id   dat)(vector-ref dat 1))
(define-inline (bkup:get-media-size dat)(vector-ref dat 2))
(define-inline (bkup:get-media-full dat)(vector-ref dat 3))
(define-inline (bkup:get-reported   dat)(vector-ref dat 4))
(define-inline (bkup:get-dir-size   dat)(vector-ref dat 5))

(define-inline (bkup:set-db!         dat val)(vector-set! dat 0 val))
(define-inline (bkup:set-media-id!   dat val)(vector-set! dat 1 val))
(define-inline (bkup:set-media-size! dat val)(vector-set! dat 2 val))
(define-inline (bkup:set-media-full! dat val)(vector-set! dat 3 val))
(define-inline (bkup:set-reported!   dat val)(vector-set! dat 4 val))
(define-inline (bkup:set-dir-size!   dat val)(vector-set! dat 5 val))

;;======================================================================
;; Data
;;======================================================================
;;
(define (lmbk:get-add-data db md5sum size)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id md5sum size)
       (set! res (list id md5sum size)))
     db "SELECT id,md5sum,size FROM data WHERE md5sum=? AND size=?;" md5sum size)
    (if res
	res
	(begin
	  (sqlite3:exec db "INSERT INTO data (md5sum,size) VALUES (?,?);" md5sum size)
	  (lmbk:get-add-data db md5sum size)))))

;;======================================================================
;; Files
;;======================================================================
;;

(define (lmbk:ftype-add-get-id db ftype)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id)
       (set! res id))
     db "SELECT id FROM ftypes WHERE ftype=?;" ftype)
    (if res
	res
	(let ((compr (get-ans (conc "Is " ftype " compressible (yes|no): "))))
	  (sqlite3:exec db "INSERT INTO ftypes (ftype,compressible) VALUES(?,?);" 
			ftype
			(if (equal? compr "no") 0 1))
	  (lmbk:ftype-add-get-id db ftype)))))

(define *file-type-cache* (make-hash-table))

;; returns the id of the file type
(define (lmbk:get-file-type db fpath)
  (let ((ftype (file-type fpath)))
    (if (hash-table-ref/default *file-type-cache* ftype #f)
	(hash-table-ref *file-type-cache* ftype)
	(let ((ftype-id (lmbk:ftype-add-get-id db ftype)))
	  (hash-table-set! *file-type-cache* ftype ftype-id)
	  ftype-id))))

(define (lmbk:compress-file? db ftype-id)
  (let ((res #f))
    (sqlite3:for-each-row 
     (lambda (compr)
       (if (eq? compr 1)
	   (set! res #t)))
     db "SELECT compressible FROM ftypes WHERE id=?;" ftype-id)
    res))

;; Returns a 13-element vector with the following contents: 
;;   0 inode-number,
;;   1 mode (as with file-permissions),
;;   2 number of hard links,
;;   3 uid of owner (as with file-owner), 
;;   4 gid of owner, 
;;   5 size (as with file-size)
;;   6 access,
;;   7 change,
;;   8 modification-time 
;;   9 device id, 
;;  10 device type
;;  11 inode, 
;;  12 blocksize
;;  13 blocks allocated

(define (lmbk:get-add-file db path fname) ;; fullname is /path/filename
  (let* ((rfull   (canonical-path (conc path "/" fname)))
	 (path-id (lmbk:get-path-id-from-parts db 0 (string-split path "/")))
	 (fstat   (file-stat rfull))
	 (fmode   (vector-ref fstat 1))
	 (fownr   (vector-ref fstat 3))
	 (gownr   (vector-ref fstat 4))
	 (fsize   (vector-ref fstat 5))
	 (fmodt   (vector-ref fstat 8))
	 (ftype   (lmbk:get-file-type db rfull))
	 (fid     (lmbk:get-file-id db path-id fname fmode fownr gownr ftype)))
    (if fid
	fid
	(begin
	  (sqlite3:exec db "INSERT INTO files (path_id,name,perms,owner_id,group_id,ftype_id) VALUES(?,?,?,?,?,?);"
			path-id
			fname
			fmode
			fownr
			gownr
			ftype)
	  (lmbk:get-file-id db path-id fname fmode fownr gownr ftype)))))

(define (lmbk:file-in-db? db path fname) ;; fullname is /path/filename
  (let* ((rfull   (canonical-path (conc path "/" fname)))
	 (path-id (lmbk:get-path-id-from-parts db 0 (string-split path "/")))
	 (res     #f))
    (sqlite3:for-each-row
     (lambda (id)
       (set! res id))
     db "SELECT id FROM files WHERE path_id=? AND name=?;" path-id fname)
    res))

(define (lmbk:file-in-db-up-to-date? db path fname) ;; fullname is /path/filename
  (let* ((rfull   (canonical-path (conc path "/" fname)))
	 (path-id (lmbk:get-path-id-from-parts db 0 (string-split path "/")))
	 (res     #f)
	 (modtime (file-modification-time (conc path "/" fname))))
    (sqlite3:for-each-row
     (lambda (id)
       (set! res id))
     db "SELECT f.id FROM files AS f INNER JOIN bkups AS b ON f.id=b.file_id WHERE f.path_id=? AND f.name=? AND b.bkup_date>=?;" 
     path-id fname modtime)
    res))

(define (lmbk:get-file-id db path-id fname fmode fownr gownr ftype)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id)
       (set! res id))
     db "SELECT id FROM files WHERE path_id=? AND name=? AND perms=? AND owner_id=? AND group_id=? AND ftype_id=?;"
     path-id
     fname
     fmode
     fownr
     gownr
     ftype)
    res))

(define *dat-id-cache* (make-hash-table))

(define  (lmbk:get-dat-id db rfull fsize)
  (if (hash-table-ref/default *dat-id-cache* rfull #f)
      (hash-table-ref *dat-id-cache* rfull)
      (let* ((md5sum (get-md5sum rfull))
	     (size   (if fsize fsize (file-size rfull)))
	     (dat    (lmbk:get-add-data db md5sum size)))
	(hash-table-set! *dat-id-cache* rfull dat)
	dat)))

;;======================================================================
;; Media
;;======================================================================
;;
(define (lmbk:get-media-code db mtype size)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id code)
       (set! res (list id code)))
     db "SELECT id,code FROM mtypes WHERE mtype=? AND size=?;" mtype size)
    res))

(define (lmbk:get-media-code db mtype size)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id code)
       (set! res (list id code)))
     db "SELECT id,code FROM mtypes WHERE mtype=? AND size=?;" mtype size)
    res))

(define (lmbk:mtype-id->media-size db mtype-id)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (code)
       (set! res code))
     db "SELECT size FROM mtypes WHERE id=?;" mtype-id)
    res))

(define (lmbk:mtype-id->media-code db mtype-id)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (code)
       (set! res code))
     db "SELECT code FROM mtypes WHERE id=?;" mtype-id)
    res))

(define (lmbk:list-mtypes db)
  (let ((res '()))
    (sqlite3:for-each-row
     (lambda (i t s c r)
       (print i ") type: " t " size: " s " code: " c " rewriteable: " (if (eq? r 0)
									  "No" "Yes"))
       (set! res (cons (list i t s c r) res)))
     db "SELECT id,mtype,size,code,rewriteable FROM mtypes;")
    res))

(define (lmbk:add-bkup-media db mtype-id)
  (let ((hn  (get-host-name))
        (nid #f)
        (mcode (lmbk:mtype-id->media-code db mtype-id))
	(msize (lmbk:mtype-id->media-size db mtype-id)))
    (bkup:set-media-size! bkupdat msize)
    (sqlite3:for-each-row
     (lambda (max-id)
       (set! nid max-id))
     db "SELECT max(id)+1 FROM media;")
    (if (not (number? nid)) ;; first time though
	(set! nid 0))
    (if nid
        (let* ((ns   (conc "00000000000000000000" nid))
               (len  (string-length ns))
               (name (conc mcode (substring ns (- len 8) len))))
	  (print "Label your media: " name)
	  (cmd->list "touch" (conc (lmbk:meta-get db "STAGING_DIR") "/" name))
          (sqlite3:exec db "INSERT INTO media (name,date_created,type_id,num_writes) VALUES(?,?,?,0);"
                        name 
                        (current-seconds)
                        mtype-id) ;; media type
	  (lmbk:mname->media-id db name)))))

(define (lmbk:mname->media-id db mname) ;; name TEXT,date_created INTEGER,type_id INTEGER,num_writes INTEGER);"
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id)
       (set! res id))
     db 
     "SELECT id FROM media WHERE name=?;" mname)
    res))

(define (lmbk:media-id->mname db media-id) ;; name TEXT,date_created INTEGER,type_id INTEGER,num_writes INTEGER);"
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (name)
       (set! res name))
     db 
     "SELECT id FROM media WHERE id=?;" media-id)
    res))

(define (lmbk:media-id->size db media-id) ;; name TEXT,date_created INTEGER,type_id INTEGER,num_writes INTEGER);"
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (name)
       (set! res name))
     db 
     "SELECT mtypes.size FROM media INNER JOIN mtypes ON media.type_id=mtypes.id WHERE media.id=?;" media-id)
    res))

;;======================================================================
;; Dirs
;;======================================================================
;;
;; (parent-id name) => id
(define *path-cache*    (make-hash-table))
;; id => (parent-id name)
(define *path-id-cache* (make-hash-table))

(define (lmbk:get-chunk-id db parent-id name)
  (let* ((path-key  (list parent-id name))
	 (pchunk-id (hash-table-ref/default *path-cache* path-key #f)))
    (if pchunk-id 
	pchunk-id
	(let ((res #f))
	  (sqlite3:for-each-row
	   (lambda (id)
	     (set! res id))
	   db
	   "SELECT id FROM dirs WHERE parent_id=? AND name=?;" parent-id name)
	  (if res
	      (begin
		(hash-table-set! *path-cache* path-key res)
		(hash-table-set! *path-id-cache* res path-key)
		res)
	      (begin
		(lmbk:add-chunk db parent-id name)
		(lmbk:get-chunk-id db parent-id name)))))))

(define (lmbk:add-chunk db parent-id name)
  (sqlite3:exec db "INSERT INTO dirs (name,parent_id) VALUES(?,?);" name parent-id))

(define (lmbk:get-path-id-from-parts db parent-id path-lst)
  (if (null? path-lst)
      parent-id
      (let* ((pchunk   (car path-lst))
	     (rempath   (cdr path-lst))
	     (pchunk-id (lmbk:get-chunk-id db parent-id pchunk)))
	(lmbk:get-path-id-from-parts db pchunk-id rempath))))

;; call with path = () to read a path
;;           pseg = name of segment
;;           path-id and parent-id
;; returns path as list of segments, use string-intersperse to
;; convert to path as string?
(define (lmbk:get-path-from-id db path-id)
  (let loop ((pdat (lmbk:get-path-parent-from-id db path-id))
	     (path '()))
    (let* ((parent-id (car pdat))
	   (segname   (cadr pdat))
	   (newpath   (cons segname path)))
      (if (eq? parent-id 0)
	  newpath ;; (string-intersperse newpath "/") ;; 
	  (loop (lmbk:get-path-parent-from-id db parent-id)
		newpath)))))

(define (lmbk:get-path-parent-from-id db path-id)
  (let ((res (hash-table-ref/default *path-id-cache* path-id #f)))
    (if res
	res
	(begin
	  (sqlite3:for-each-row
	   (lambda (parent-id name)
	     (set! res (list parent-id name)))
	   db "SELECT parent_id,name FROM dirs WHERE id=?;"
	   path-id)
	  (hash-table-set! *path-id-cache* path-id res)
	  (hash-table-set! *path-cache* res path-id)
	  res))))

(define (lmbk:get-path-segment-info db path-id)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (id name parent-id perms owner)
       (set! res (list id name parent-id perms owner)))
     db "SELECT id,name,parent_id,perms,owner FROM dirs WHERE id=?;"
     path-id)
    res))

(define (lmbk:search-files db patt)
  (let ((filedat '()))
    (sqlite3:for-each-row
     (lambda (id path-id name media-name md5sum)
       (let ((path (conc "/" (string-intersperse (lmbk:get-path-from-id db path-id) "/") "/" name)))
	 (set! filedat (cons (list id path media-name md5sum) filedat))))
     db
     "SELECT f.id,f.path_id,f.name,m.name,d.md5sum FROM files AS f INNER JOIN bkups AS b ON f.id=b.file_id INNER JOIN data AS d ON b.data_id=d.id INNER JOIN media AS m ON b.media_id=m.id WHERE f.name LIKE ?;" patt)
    filedat))

;; block for deleting a bkups entry needed!
;; SELECT bkups.id,data.id FROM bkups INNER JOIN data ON bkups.data_id=data.id WHERE data.md5sum LIKE '7987cfad1ab3d5dde29616473584%';


(define (lmbk:search-files-by-md5sum db patt)
  (let ((filedat '()))
    (sqlite3:for-each-row
     (lambda (id path-id name media-name)
       (let ((path (conc "/" (string-intersperse (lmbk:get-path-from-id db path-id) "/") "/" name)))
	 (set! filedat (cons (list id path media-name) filedat))))
     db
     (conc "SELECT files.id,files.path_id,files.name,media.name FROM bkups "
	   "INNER JOIN files ON bkups.file_id=files.id "
	   "INNER JOIN data ON bkups.data_id=data.id "
	   "INNER JOIN media ON bkups.media_id=media.id "
	   "WHERE data.md5sum like ?;")
     patt)
    filedat))

;;======================================================================
;; Setup stuff
;;======================================================================
;;
(define (lmbk:meta-get db key)
  (let ((res #f))
    (sqlite3:for-each-row
     (lambda (val)
       (set! res val))
     db "SELECT val FROM meta WHERE key=?;" key)
    res))

(define (lmbk:meta-set! db key val)
  (let ((existing (lmbk:meta-get db key)))
    (if existing
	(sqlite3:exec db "UPDATE meta SET val=? WHERE key=?;" val key)
	(sqlite3:exec db "INSERT INTO meta (key,val) VALUES(?,?);" key val))))

(define (lmbk:meta-del! db key)
  (sqlite3:exec db "DELETE FROM meta WHERE key=?;" key))

(define (lmbk:report-setup db)
  (sqlite3:for-each-row
   (lambda (k v)
     (print k " => " v))
   db "SELECT key,val FROM meta ORDER BY key ASC;"))

;;======================================================================
;; Misc
;;======================================================================
;;
(define (get-ans prompt)
  (display prompt)(flush-output)
  (let ((ans (read-line)))
    ans))

;; DONT USE - NEEDS map conc path "/" p stuff
(define (get-files recursive . paths) 
  (if (not recursive)
      (car (append (map
		    (lambda (path)
		      (filter (lambda (d)
				(not (directory? d)))
			      (directory path #t)))
		    paths)))
      (append (apply get-files #f paths)
	      (car (append (map (lambda (path)
				  (get-files #t path))
				(filter (lambda (d)
					  (directory? d))
					paths)))))))

(define (proc-on-files proc recursive . paths) ;; REPLACE WITH find-file
  (if (not recursive)
      (for-each ;; process files only (i.e. non-recursive)
       (lambda (path)
	 (for-each
	  (lambda (f)
	    (proc f))
	  (filter (lambda (d)
		    (and (not (directory? d))
			 (not (symbolic-link? d))))
		  (map
		   (lambda (p)
		     (conc path "/" p))
		   (directory (canonical-path path) #t)))))
       (filter (lambda (p)(and (file-read-access? p)
			       (regular-file? p)))
	       paths))
      (begin
	;; process the files in the sub dirs
	(apply proc-on-files proc #f paths)
	;; now run on each dir
	(for-each
	 (lambda (path)
	   (let ((subdirs (filter (lambda (d)
				    (and (directory? d)
					 (if (and (file-read-access? d)
						  (not (symbolic-link? d)))
					     #t
					     (begin
					       (print "WARNING: Skipping unreadable path " d)
					       #f))))
				  (map
				   (lambda (p)
				     (conc path "/" p))
				   (directory (canonical-path path) #t)))))
	     (apply proc-on-files proc #t subdirs)))
	 paths))))

;;======================================================================
;; Running system commands
;;======================================================================

(define (cmd->list cmd . params)
  ;;  (with-exception-handler 
  ;;		     (lambda (exn)
  ;;		       (print exn)
  ;;		       '("")) ;; (print "Yuk"))
  (let-values (((inp oup pid)  (process cmd params)))
	      (let ((res (port->list inp)))
		;; (status (close-input-pipe fh)))
		(close-input-port inp)
		(close-output-port oup)
		res)))
;; (if (null? res)
;; 	'("") ;; this makes coding easier
;;	res))) ;; ) ;; (if (eq? status 0) res #f)))

(define (cmd-run-proc-each-line cmd proc . params)
  (let* ((fh (process cmd params)))
    (let loop ((curr (read-line fh))
	       (result  '()))
      (if (not (eof-object? curr))
	  (loop (read-line fh)
		(append result (list (proc curr))))
	  result))))

(define (cmd-run-proc-each-line-alt cmd proc)
  (let* ((fh (open-input-pipe cmd))
	 (res (port-proc->list fh proc))
	 (status (close-input-pipe fh)))
    (if (eq? status 0) res #f)))

(define (port->list fh)
  (if (eof-object? fh) #f
      (let loop ((curr (read-line fh))
		 (result '()))
	(if (not (eof-object? curr))
	    (loop (read-line fh)
		  (append result (list curr)))
	    result))))

(define (port-proc->list fh proc)
  (if (eof-object? fh) #f
      (let loop ((curr (proc (read-line fh)))
		 (result '()))
	(if (not (eof-object? curr))
	    (loop (let ((l (read-line fh)))
		    (if (eof-object? l) l (proc l)))
		  (append result (list curr)))
	    result))))

;;======================================================================
;; File utilities
;;======================================================================

;; switch to string-sub-many since string-substitute* doesn't work everywhere
(define (filename-escape f)
  (string-sub-many f '(("\\(" . "\\(") 
		       ("\\)" . "\\)")
		       ("\\[" . "\\[")
		       ("\\]" . "\\]")
		       ("\\{" . "\\{")
		       ("\\}" . "\\}")
		       ("\\$" . "\\$")
		       ("\\?" . "\\?")
		       (" " . "\\ ")
		       ("'" . "\\'")
		       ("\""  . "\\\"")
		       ("&" . "\\&")
		       ("#" . "\\#")
		       ("@" . "\\@"))))

(define (get-md5sum file)
  (if (directory? file) "" 
      (car (string-split 
	    (car (cmd->list "md5sum" file))))))

(define (dir-size path)
  ;; (print "dir-size: " path)
  (let ((du (cmd->list "du" "-sk" path)))
    ;; (print "dir-size du: " du)
    (if (null? du)
	99999999999
	(string->number 
	 (car (string-split 
	       (car du) "\t"))))))

(define (file-type path)
  (car (cmd->list "file" "-bi" path)))

(define (string-sub-many str lst)
  (let loop ((str  str)
	     (rule (car lst))
	     (tail (cdr lst)))
    (let ((r (car rule))
	  (s (cdr rule)))
      ;; (print "r=" r ", s=" s ", str=" str)
      (if (null? tail)(string-substitute r s str #t)
	  (loop (string-substitute r s str #t)
		(car tail)(cdr tail))))))

;;======================================================================
;; Backup areas
;;======================================================================
;;
;; recursive is 1 for yes, 0 for no
(define (lmbk:register-bkup-path db path recursive)
  (let* ((path-id  (lmbk:get-path-id db (canonical-path path)))
	 (hostname (get-host-name))
	 (bkpid    (lmbk:get-bkup-path-id db hostname path-id recursive)))
    (if bkpid
	bkpid
	(begin
	  (sqlite3:exec db "INSERT INTO bkpaths (host,path_id,recursive,lastdate) VALUES(?,?,?,0);" hostname path-id recursive)
	  (lmbk:get-bkup-path-id db hostname path-id recursive)))))

(define (lmbk:get-bkup-path-id db host path-id recursive)
  (let ((pid #f))
    (sqlite3:for-each-row
     (lambda (id)
       (set! pid id))
     db "SELECT id FROM bkpaths WHERE host=? AND path_id=? AND recursive=?;"
     host path-id recursive)
    pid))

(define (lmbk:get-oldest-bkup-path-id db host)
  (let ((pid #f))
    (sqlite3:for-each-row
     (lambda (id path-id recursive lastdate)
       (set! pid (list id path-id recursive lastdate)))
     db "SELECT id,path_id,recursive,lastdate FROM bkpaths WHERE host=? ORDER BY lastdate ASC LIMIT 1;"
     host)
    pid))

(define (lmbk:set-bkup-date db bkup-id date)
  (sqlite3:exec db "UPDATE bkpaths SET lastdate=? WHERE id=?;" date bkup-id))

(define (lmbk:get-path-id db path)
  (let ((path-lst (string-split path "/")))
    (lmbk:get-path-id-from-parts db 0 path-lst)))

(define (lmbk:list-bkup-areas db printem host)
  (let ((res'()))
    (sqlite3:for-each-row
     (lambda (id path-id recursive lastdate)
       (let ((path (conc "/" (string-intersperse (lmbk:get-path-from-id db path-id) "/"))))
	 (if printem
	     (print id " " path " " (if (eq? recursive 1) "recursive" "") " " lastdate))
	 (set! res (append res (list (list id path-id path recursive))))))
     db "SELECT id,path_id,recursive,lastdate FROM bkpaths WHERE host like ? ORDER BY lastdate DESC;" host)
    res))

(define (lmbk:get-oldest-bkup-area db host)
  (let* ((area-info    (lmbk:get-oldest-bkup-path-id db host))
	 (area-id      (if (or (not area-info)
			       (null? area-info))
			   (begin
			     (print "ERROR: No backup areas defined! Please registers some areas with -r or -a")
			     (exit 1))
			   (car area-info)))
	 (area-path-id (cadr area-info))
	 (recursive    (caddr area-info))
	 (lastdate     (cadddr area-info)))
    (list 
     area-id
     area-path-id
     (canonical-path (conc "/" (string-intersperse (lmbk:get-path-from-id db area-path-id) "/")))
     recursive
     lastdate)))

;;======================================================================
;; Bkups
;;======================================================================
;;

(define (lmbk:register-bkup-file db file-id data-id media-id compressed)
  (sqlite3:exec
   db 
   "INSERT INTO bkups (file_id,data_id,media_id,compressed,bkup_date) VALUES(?,?,?,?,?);"	
   file-id data-id media-id (if compressed 1 0)(current-seconds)))

;; returns number of media (i.e. disks) using this same data
;;
(define (lmbk:data-already-bkuped db datid)
  (let ((res '()))
    (sqlite3:for-each-row
     (lambda (media-id)
       (set! res (cons media-id res)))
     db "SELECT media_id FROM bkups WHERE data_id=?;" datid)
    res))

;; level; 0=file has been backed up
;;        1=0 + data unchanged
(define (lmbk:file-backedup? db path fname level bkupdate) 
  (let* ((res #f)
	 (fullp (conc path "/" fname))
	 (fstat (file-stat fullp))      ;; yep, this stuff is redundant
	 (fmodt (vector-ref fstat 8)))  ;; consolidate it all
    (case level
      ((0) ;; file is merely mentioned in the db
       (if (and (and bkupdate (> bkupdate fmodt))
	        (lmbk:file-in-db? db path fname))
	   (set! res #t)))
      ((1)
       (if (lmbk:file-in-db-up-to-date? db path fname)
	   (set! res #t)))
      ((2)
       (let ((fid (lmbk:get-add-file db path fname))
	     (did (lmbk:get-dat-id db fullp #f)))
	 (sqlite3:for-each-row
	  (lambda (id)
	    (set! res id))
	  db "SELECT id FROM bkups WHERE file_id=? AND data_id=?;"
	  fid (car did))))
      ((2) ;; now make sure there is a duplicate on another disk
       #f) ;; not implemented yet :-)
      (else #f))
    res))

(define (lmbk:put-in-staged-area db path fname media-id)
  (let* ((rfull    (canonical-path (conc path "/" fname)))
	 (datid    (lmbk:get-dat-id db rfull #f)) ;; (list id md5sum size)
	 (md5sum   (cadr datid))
	 (d1       (substring md5sum 0 1))
	 (d2       (substring md5sum 1 2))
	 (dups     (lmbk:data-already-bkuped db (car datid)))
	 (numdups  (length dups))
	 (size     (file-size rfull))
	 (compr    (if (> size 2e3) ;; less than 2k? no point in compressing
		       (lmbk:compress-file? db (lmbk:get-file-type db rfull))))
	 (fid      (lmbk:get-add-file db path fname)) ;; this is BADLY redundant FIXME
	 (stagedir (lmbk:meta-get db "STAGING_DIR"))
	 (targd    (conc stagedir "/" d1 "/" d2))
	 (targf    (conc targd "/" md5sum))
	 (file2big #f))
    (if (not (file-exists? (conc stagedir "/" d1)))
	(create-directory (conc stagedir "/" d1)))
    (if (not (file-exists? targd))
	(create-directory targd))
    (if (not (lmbk:have-space? db (/ size 1e3))) ;; convert size in bytes to kib
	(set! file2big #t))
    (case numdups
      ((0)
       (if (not file2big)
	   (begin
	     (file-copy rfull targf)
	     (if compr
		 (cmd->list "bzip2" targf))
	     (lmbk:register-bkup-file db fid (car datid) media-id compr))
	   (print "Skipping file " rfull ", it doesn't fit on this backup")))
      (else
       ;; note: should never get here since this decision is made elsewhere.
       (print "WARNING: Not backing up file " rfull " as it has data already backed up")
       (lmbk:register-bkup-file db fid (car datid) (car dups) compr)))
    (if (not (lmbk:have-space? db 0)) ;;  (/ size 1e3)))
	(if (not (bkup:get-media-full bkupdat))
	    (begin
	      (print "MEDIA NOW FULL")
	      (bkup:set-media-full! bkupdat #t))))))

(define (lmbk:have-space? db adjval)
  ;; now check if there is space left for more data
  (let ((msize    (* 1024000 (bkup:get-media-size bkupdat)))
	(dbsize   (quotient (file-size "~/.lmbk/lmbk.db") 1000)) ;; 1000 or 1024?
	(prevused (bkup:get-dir-size bkupdat)))
    ;; (print "msize: " msize " dbsize: " dbsize " prevused: " prevused " adjval: " adjval)
    (if (and prevused
	     (> msize (+ (* prevused 0.01) prevused adjval dbsize)))
	(begin
	  (bkup:set-dir-size! bkupdat (+ prevused adjval))
	  #t)
	(let ((used  (dir-size (lmbk:meta-get db "STAGING_DIR"))))
	  ;; (print "msize: " msize " dbsize: " dbsize " prevused: " prevused " used: " used " adjval: " adjval)
	  (bkup:set-dir-size! bkupdat used)
	  (> msize (+ (* used 0.01) used adjval dbsize))))))

(define (lmbk:run-bkup db mtype-id media-id) ;; media type and size specified
  (let ((stagedir  (lmbk:meta-get db "STAGING_DIR"))
	(ldb       #f) ;; the metadata db for the backup disk
	(freespace #f)
	(host      (get-host-name)))
    ;; create stage area 
    (if (file-exists? stagedir)
	(begin
	  (if (> (length (directory stagedir #t)) 0)
	      (begin
		(print "WARNING: Staging directory " stagedir " is not empty!")
		(if (not (equal? "yes" (get-ans "Enter yes to continue: ")))
		    (exit))))
	  (print "Using existing directory for staging: " stagedir)
	  (if (not (equal? (get-ans "Enter yes to continue: ") "yes"))
	      (begin
		(print "Quiting...")
		(exit))))
	(create-directory stagedir))

    (if (eq? (length (lmbk:list-bkup-areas db #f host)) 0)
	(begin
	  (print "You must add some areas to be backed up!")
	  (print "use the -b and -r switches")
	  (print help)
	  (exit)))

    ;; do this *after* checking for bkup areas
    (if (not media-id)
	(set! media-id (lmbk:add-bkup-media db mtype-id))
	(let ((msize (lmbk:media-id->size db media-id))
	      (mname (lmbk:media-id->mname db media-id)))
	  (bkup:set-media-size! bkupdat msize)
	  (print "WARNING: Continuing on media " mname)))
    
    (bkup:set-media-full! bkupdat #f) ;; the media full flag

    ;; create db in stage area 
    ;; for now I will merely copy the db from the .lmbk area
    ;; (set! ldb (lmbk:mk-db mk-bkdb-tables (conc stagedir "/lmbk.db")))

    ;; pass 1, files not backed up at all
    
    (let loop ((bkarea   (lmbk:get-oldest-bkup-area db host))
	       (numareas (length (lmbk:list-bkup-areas db #f host)))
	       (count    0)
	       (bkmode   0))

      (print "Total areas: " numareas " Backing up from area: " bkarea)
      ;; reset this at the begining of the backup so the next run starts
      ;; in a different dir, whether or not this run completed.
      ;; NO!! do it later so that incompletely backed up areas are completed before moving on
      ;;; (lmbk:set-bkup-date db (car bkarea) (current-seconds))

      ;; reset the path caches, bulk of files are probably different
      (set! *path-cache*    (make-hash-table))
      (set! *path-id-cache* (make-hash-table))

      (find-files      ;; used to be proc-on-files 
       (caddr bkarea)  ;; path to start find
       (lambda (f)
	 (and (regular-file? f)
	      (file-read-access? f)));; predicate
       (lambda (fpath blah)
	 (if (or (bkup:get-media-full bkupdat)
		 (and (file-exists? fpath)
		      ;; (file-is-readable? fpath)
		      (symbolic-link? fpath)))
	     (if (bkup:get-media-full bkupdat)
		 (if (not (bkup:get-reported bkupdat))
		     (begin
		       (print "Staging dir now full.")
		       (bkup:set-reported! bkupdat #t)))
		 (print "Skipping " fpath))
	     (begin
	       (format #t "~a ~a " bkmode fpath)
	       (let* ((path  (pathname-directory fpath))
		      (fname (pathname-strip-directory fpath))
		      (bkdup (lmbk:file-backedup? db path fname bkmode (cadddr bkarea))))
		 (if bkdup
		     (print " (skipped, " (case bkmode
					    ((0) "filename exists in db)")
					    ((1) (conc "file not touched since last backup of " (cadddr bkarea) ")"))
					    ((2) (conc "data not changed)"))
					    ((3) "data already duplicated on multiple disks)")))
		     (begin
		       (print " (done)")
		       (lmbk:put-in-staged-area db path fname media-id)))))))
       #t                   ;; identity            
       (if (eq? (list-ref bkarea 3) 0)
	   0  ;; (find-files "/home/matt/.wine/dosdevices/c:/windows/profiles/matt/My\ Music/.kde-safe/cache-Knoppix/http" regular-file? print #f (lambda (d)(print "inspecting " d)(and (file-read-access? d)(not (symbolic-link? d))))) 
	   (lambda (d)
	     (and (file-read-access? d)
		  (not (symbolic-link? d))))) ;; recursive is actually #f, not #t as you might expect (it makes sense after thinking about it.
       )
      (set! freespace (not (bkup:get-media-full bkupdat))) ;; silly, switch this back, get-media-full is cheap

      ;; Not sure yet that this is the right place to mark the backup as done
      (lmbk:set-bkup-date db (car bkarea) (current-seconds)) ;; fully done with this block. 

      (if (and freespace
	       (> numareas count))
	  (loop (lmbk:get-oldest-bkup-area db (get-host-name))
		numareas
		(+ count 1)
		bkmode;; this will be incremented etc. in future
		)
	  ;; here is where you loop for different bkmodes
	  (if (and freespace (< bkmode 3))
	      (loop (lmbk:get-oldest-bkup-area db (get-host-name))
		    numareas
		    0 ;; start at beginning
		    (+ bkmode 1)))
	  ))
    ;; now put the lmbk.db file on the disk
    (file-copy "~/.lmbk/lmbk.db" (conc (lmbk:meta-get db "STAGING_DIR") "/lmbk.db") #t)

    ;; pass 2, files changed since last backup
    ;; pass 3, refresh files on old backup media

    ;; while space used < space available
    ;;    link (if possible) data over
    ;;    compress data if viable
    ;;    store info in master db and media db
    #t))

;;======================================================================
;; MAIN
;;======================================================================

(define db (lmbk:mk-db mk-bkdb-tables dbpath))
(define bkupdat (make-vector 10 #f))
(bkup:set-db! bkupdat db)

;; set variables
(if (get-arg "-s")
    (if (> (length remargs) 0)
	(lmbk:meta-set! db (get-arg "-s") (car remargs))
	(lmbk:meta-del! db (get-arg "-s"))))

(if (not (lmbk:meta-get db "STAGING_DIR"))
    (begin
      (print "Please set the STAGING_DIR variable")
      (print help)
      (exit)))

;; register paths for backup
(if (get-arg "-b")
    (if (file-exists? (get-arg "-b"))
	(let ((recurs (if (get-arg "-r") 1 0)))
	  (lmbk:register-bkup-path db (get-arg "-b") recurs))
	(begin
	  (print "ERROR: Can't add path " (get-arg "-b") " path not found")
	  (exit))))

;; create staging dir and put files to be backed up
;; in it.
(if (get-arg "-p")
    (let ((mtypes   (lmbk:list-mtypes db))
	  (mtype-id (string->number (get-ans "Enter the number for the media you will use: "))))
      (if (assoc mtype-id mtypes) ;;  (list i t s c r)
	  (let ((mdat (assoc mtype-id mtypes)))
	    (lmbk:run-bkup db mtype-id #f)) ;; (cadr mdat)(caddr mdat)))
	  (print "ERROR: You didn't select an existing option." mtype-id))))

(if (get-arg "-cont")
    (let* ((bkupname (last (string-split (car (glob (conc (lmbk:meta-get db "STAGING_DIR") "/[A-Z]*0*"))) "/")))
	   (media-id (lmbk:mname->media-id db bkupname)))
      (lmbk:run-bkup db #f media-id)))

(if (get-arg "-lsbkups")
    (let ((host (if (> (length remargs) 0)
		    (car remargs)
		    (get-host-name))))
      (lmbk:list-bkup-areas db #t host)))

(if (get-arg "-find")
    (for-each
     (lambda (dat)
       (print (caddr dat) " " (cadddr dat) " " (cadr dat) ))
     (lmbk:search-files db (get-arg "-find"))))

(if (get-arg "-lookup")
    (for-each
     (lambda (dat)
       (print dat))
	 (lmbk:search-files-by-md5sum db (get-arg "-lookup"))))

(sqlite3:finalize! db)
