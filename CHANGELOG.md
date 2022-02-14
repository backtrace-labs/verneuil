0.4.2 2022-02-14
----------------
* Try to detect when the boot time changes and still reuse the old
  spooling directory path for the current boot.

0.4.1 2022-02-12
----------------
* Add a field in manifests to describe the program/library version
  that generated each manifest (f855c656)

0.4.0 2022-02-11
----------------
* Backward-incompatible tweak to the new manifest format,
  before the old protobuf spec sees too much use (9e1183c3)
* Use a base chunk for the fingerprint list more aggressively
  (24746b30)

0.3.0 2022-02-11 (YANKED for proto spec tweak)
----------------
* New manifest format now includes the first chunk (which changes
  after every successful sqlite write transaction) in order to save
  API calls for reads and writes (8e7fa62b) [#7]
* More tentative support for long paths (> `MAX_MAX`) (76519c99)

0.2.0 2022-02-08
----------------
* New, better compressible, manifest format, to reduce the manifest
  overhead for large (1+ GB) sqlite files (internal docs in edacfeb6) [#3]

0.1.5 2022-02-06
----------------
* Be more resilient against S3 clones where blobs may flicker in
  and out of existence shortly after PUTs (320390a4)
* More efficient chunk loads and patrol refreshes, with a cached
  executor instead of ephemeral ones (b7d4b610, ae8ff325)

0.1.4 2022-01-31
-----------------
* Enable dynamic linking against older sqlites (b834c18d)
* Simplify the logic to avoid low fd numbers (2be8aeac)
* Support long paths (> `NAME_MAX`) (5689ff3d) [#1]
* Upgrade to governor 0.4 to drop one dependency on hash brown.

0.1.3 2021-12-07
----------------
* Make it possible to build the `libverneuil_vfs` and `verneuilctl`
  "examples" out of the `verneuil` tree, for example with (839265a)

  ```
  $ cargo build --release -p verneuil --examples --features='verneuil/dynamic_vfs,verneuil/tracing-subscriber'
  $ cargo build --release -p verneuil --examples --features='verneuil/vendor_sqlite,verneuil/tracing-subscriber,verneuil/structopt,tracing-subscriber/env-filter'
  ```

0.1.2 2021-12-06
----------------
* Register the snapshot VFS from `verneuil::configure` (d24a523)

0.1.1 2021-12-06
----------------
* Make it possible to fully populate the config from Rust (c5e067c)

0.1.0 initial release (2021-12-02)
----------------------------------
