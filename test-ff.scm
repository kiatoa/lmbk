(use posix)

(find-files "." regular-file? (lambda (x y)(print x)(print (file-stat x))))     
