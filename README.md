IDOL Committer
==============

Micro Focus IDOL implementation of Norconex Committer.  

Website: https://opensource.norconex.com/committers/idol/

## Compatibility Matrix

| This Committer   | Committer Core | HTTP Collector | FS Collector |
| ---------------- | -------------- | -------------- | ------------ |
| **2.x**          | 2.x            | 2.x            | 2.x          |
| **3.x** (master) | 3.x            | 3.x            | -            |


## Notes

* Micro Focus IDOL was previously known as HP IDOL or Autonomy IDOL. 
* For all unit tests to run, an IDOL instance must be available and 
  referred to using the following system properties (shown here with 
  sample IDOL locations):
    
    -Didol.index.url="http://localhost:9001" \
    -Didol.aci.url="http://localhost:9000"

