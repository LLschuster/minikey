# Distributed key/value store Minikey

```
Should allow storing at least 1 mb values, that can be accesible by multiple machines.
```
# TODO

```
build api for inserting, deleting and getting by key
save values in files  --> how to structure this files?
how to better lookup the keys --> implement an index, read?
how to better write the key/value --> insert in batches
handle concurrent accesses --> mutex, locks --> lockfree algo?
values can be of different types, not just primitives (arrays, maps, etc. )
```