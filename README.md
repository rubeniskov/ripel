# RIPel

A Rust library for building Domain Specific Languages (DSLs) using MiniJinja as an expression engine.


## Run examples

To run the examples, navigate to the root of the repository and use Cargo to run the desired example. For instance, to run the querier example:

```shell
cargo run -p ripel-examples --example
cargo run -p ripel-examples --example querier -- "query('Ronda').limit(1)"
```

## Domain Specific Language (DSL) Example

Shows how to use MiniJinja as a Domain Specific Language (DSL).  It demonstrates primarily the use of expressions with
custom objects to implement a simple query builder.  The script takes one argument which is
the query expression.  The environment defines a single function `query` which returns a
query object with the following methods:

- `filter` which takes keyword arguments to filter
- `limit` which limits the query to N items
- `offset` which offsets the query

Additionally this demo also implements this with filter syntax rather than methods.
This means you can also write `query("foo").filter(x=42)` or `query("foo")|filter(x=42)`
interchangeably.

The resulting query is printed.

```console
$ cargo run -- "query('my_table').filter(is_active=true)"

$ cargo run -- "query('my_table').filter(is_active=true).limit(10)"

$ cargo run -- "query('my_table') | filter(is_active=true)"

$ cargo run -- "query('my_table') | filter(is_active=true) | limit(10)"
```

This type of approach can be used to implement any form of expression evaluation that
fits into the runtime model of MiniJinja.  For instance you can use MiniJinja expressions
to implement things like CI or build-tool configuration.
