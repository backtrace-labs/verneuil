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
