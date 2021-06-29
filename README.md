# btrdb - B-tree DataBase

- [x] B-Tree
- [x] Fully [Copy-on-Write](https://en.wikipedia.org/wiki/Copy-on-write) and [log-structured](https://en.wikipedia.org/wiki/Log-structured_file_system)
- [x] Snapshots
    - [ ] Named snapshots
- [x] Key-Value sets
- [x] Document sets
    - [ ] Indexes
    - [ ] BSON instead of JSON on disk
- [x] AC<del>I</del>D
    - [ ] Isolation with concurrent reader
    - [ ] Concurrent writer (?)
- [ ] Client / Server (?)
- [ ] Replication (?)
- [ ] GC (?)

## Usage

> ⚠️ Warning! ⚠️
>
> This project is just started. It's under heavy development!
>
> The on-disk structre and the API are NOT stable yet.
>
> Please do NOT use it in any serious production.

See `test.ts`.

## Design

![design.svg](./docs/design.svg)
