
```
cargo run -p ripel -- "query('Ronda').limit(1)"
```

```
kind=Map debug=<Query table="Ronda">
[0] Ronda: Round {
    id: Ulid(
        2125180292336646815349684362278833309,
    ),
    tournament_id: Ulid(
        1990790076568570403723753282058221315,
    ),
    started_at: Some(
        "2022-03-09T12:39:59Z",
    ),
    finished_at: Some(
        "2022-03-16T12:39:59Z",
    ),
    ordinal: 1,
    modality: Individual,
    created_at: "2025-09-15T03:45:36Z",
    updated_at: "2025-09-15T03:45:36Z",
}
```