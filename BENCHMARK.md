Single-node benchmark done using this [tool](https://github.com/antirez/mc-benchmark/) on my laptop:

# v0.1
OneCache:
```
./mc-benchmark -n 50000
====== SET ======
  50000 requests completed in 1.16 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

4.49% <= 0 milliseconds
81.70% <= 1 milliseconds
98.84% <= 2 milliseconds
99.80% <= 3 milliseconds
99.97% <= 4 milliseconds
99.99% <= 5 milliseconds
99.99% <= 6 milliseconds
100.00% <= 7 milliseconds
42992.26 requests per second

====== GET ======
  50007 requests completed in 1.27 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

3.90% <= 0 milliseconds
73.76% <= 1 milliseconds
97.94% <= 2 milliseconds
99.52% <= 3 milliseconds
99.93% <= 4 milliseconds
99.97% <= 5 milliseconds
99.98% <= 6 milliseconds
99.99% <= 7 milliseconds
99.99% <= 8 milliseconds
100.00% <= 9 milliseconds
39375.59 requests per second
```

memcached:
```
./mc-benchmark -n 50000
====== SET ======
  50000 requests completed in 1.00 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

5.33% <= 0 milliseconds
95.73% <= 1 milliseconds
99.58% <= 2 milliseconds
99.95% <= 3 milliseconds
99.99% <= 4 milliseconds
100.00% <= 5 milliseconds
49800.80 requests per second

====== GET ======
  50000 requests completed in 1.09 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

7.93% <= 0 milliseconds
86.05% <= 1 milliseconds
98.86% <= 2 milliseconds
99.79% <= 3 milliseconds
99.90% <= 4 milliseconds
99.90% <= 5 milliseconds
99.92% <= 8 milliseconds
99.95% <= 9 milliseconds
99.97% <= 10 milliseconds
99.99% <= 11 milliseconds
100.00% <= 12 milliseconds
100.00% <= 13 milliseconds
45703.84 requests per second
```
