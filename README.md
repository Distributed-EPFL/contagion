# contagion

This crate provides an implementation of the Contagion probabilistic secure broadcast algorithm on top of the drop crate.

# Using contagion

Currently contagion is not published on crates.io but you can still use the pre-release version using git 

``` toml
contagion = { git = "https://gihub.com/Distributed-EPFL/contagion", branch = "batched" } # for the batched version
contagion = { git = "https://gihub.com/Distributed-EPFL/contagion" } # for the classic non batched version
```

Documentation can be generated locally on your machine after cloning the repository using 

``` sh
cargo doc --open
```
