# Changelog

## [1.2.1](https://github.com/rudderlabs/keydb/compare/v1.2.0...v1.2.1) (2025-10-08)


### Miscellaneous

* **deps:** bump actions/setup-go from 5 to 6 ([#60](https://github.com/rudderlabs/keydb/issues/60)) ([22cd206](https://github.com/rudderlabs/keydb/commit/22cd2062fe4d11488957d51574f043b1d5f279a0))
* **deps:** bump actions/stale from 9 to 10 ([#59](https://github.com/rudderlabs/keydb/issues/59)) ([3e88330](https://github.com/rudderlabs/keydb/commit/3e88330351ad4d1d9ae0700d2fab6ff93ec2504f))
* **deps:** bump the go-deps group across 1 directory with 2 updates ([#63](https://github.com/rudderlabs/keydb/issues/63)) ([ec81c2b](https://github.com/rudderlabs/keydb/commit/ec81c2b9df36180de3e3b763c8537df79a810551))
* **deps:** bump the go-deps group across 1 directory with 3 updates ([#69](https://github.com/rudderlabs/keydb/issues/69)) ([ebc2caa](https://github.com/rudderlabs/keydb/commit/ebc2caaf656b4fbf253649e10089c32de1dfd614))
* force skip listing files ([#70](https://github.com/rudderlabs/keydb/issues/70)) ([fc3180b](https://github.com/rudderlabs/keydb/commit/fc3180b5a685a6e460709ad804232e1ca7099f26))
* rename changelog-types to changelog-sections in release-please config ([#72](https://github.com/rudderlabs/keydb/issues/72)) ([f6bced3](https://github.com/rudderlabs/keydb/commit/f6bced3e6fa0c14061f8ad8d12790b298e55222e))
* update release-please action to use googleapis namespace ([#71](https://github.com/rudderlabs/keydb/issues/71)) ([a5c4a46](https://github.com/rudderlabs/keydb/commit/a5c4a4646ca7545bb77fb35e9654a29872f8c609))

## [1.2.0](https://github.com/rudderlabs/keydb/compare/v1.1.0...v1.2.0) (2025-09-30)


### Miscellaneous Chores

* empty commit to fix release ([#65](https://github.com/rudderlabs/keydb/issues/65)) ([e357f87](https://github.com/rudderlabs/keydb/commit/e357f878a577526d745cea9aef8a287b183edc1b))

## [1.1.0](https://github.com/rudderlabs/keydb/compare/v1.0.0...v1.1.0) (2025-09-19)


### Features

* add Helm chart and deployment setup for KeyDB Operator ([9da2183](https://github.com/rudderlabs/keydb/commit/9da2183c32eca2a1977cf1c1eafe4abc8ea39f92))
* add more observability and metrics ([#6](https://github.com/rudderlabs/keydb/issues/6)) ([a9c50b1](https://github.com/rudderlabs/keydb/commit/a9c50b1244202567954d57e001f90bc4481cf57c))
* adding stats ([0820dea](https://github.com/rudderlabs/keydb/commit/0820dea032fb8c7cbae12ad7fcb129deb8768171))
* cuckoo filters ([2c9f6c4](https://github.com/rudderlabs/keydb/commit/2c9f6c467cb8d806d1a512c80bd34f17c8bebf5c))
* implement Badger metrics collector ([#5](https://github.com/rudderlabs/keydb/issues/5)) ([a4997b9](https://github.com/rudderlabs/keydb/commit/a4997b9f543477ab27415dd99ac3e7104ea2360b))
* implement GitHub Actions workflows and improve snapshot functionality ([69f2374](https://github.com/rudderlabs/keydb/commit/69f23748d1b552da6d4f59beac13d9e2d160ac9a))
* implement rollback functionality for scaling operations ([#23](https://github.com/rudderlabs/keydb/issues/23)) ([dd119e7](https://github.com/rudderlabs/keydb/commit/dd119e7205cc355ec17a41349969ae284a93d1ff))
* improve keydb write latencies ([#9](https://github.com/rudderlabs/keydb/issues/9)) ([97e8501](https://github.com/rudderlabs/keydb/commit/97e850102b2db088827a215e9362b287b3fc1894))
* load snapshots ([479336c](https://github.com/rudderlabs/keydb/commit/479336c6052290ac0bdee77cbd7b7ca2ecbde96d))
* **node:** add configurable backup folder name and refactor snapshot file path ([#33](https://github.com/rudderlabs/keydb/issues/33)) ([d79c882](https://github.com/rudderlabs/keydb/commit/d79c8829babcf62b714d3181b031810b59976f23))
* operator ([27cbeae](https://github.com/rudderlabs/keydb/commit/27cbeaeffb70f50f8dbcd3bc322e6cf60e7c80b2))


### Bug Fixes

* cache initialization ([a584b7b](https://github.com/rudderlabs/keydb/commit/a584b7b3ab213ca925312a6f92f0e030610fe7ea))
* data races ([d851213](https://github.com/rudderlabs/keydb/commit/d8512138964d3579858c6f7cb7a25cacc7a85f02))
* get/put cluster size changes ([5fcc9d4](https://github.com/rudderlabs/keydb/commit/5fcc9d4374e425b39f84fdceaa09193bb9335dc4))
* growth keys ([3d041fe](https://github.com/rudderlabs/keydb/commit/3d041fef76e80804352901244e6c101b476ae6b8))
* handle context cancellation in snapshot creation ([#39](https://github.com/rudderlabs/keydb/issues/39)) ([9cd22ab](https://github.com/rudderlabs/keydb/commit/9cd22abe3d946e9f049505dc9da701292b31f3da))
* load snapshots ([51740bb](https://github.com/rudderlabs/keydb/commit/51740bb3a5ef346909471479ff7caf20d8b1e452))
* node ([00886a3](https://github.com/rudderlabs/keydb/commit/00886a31baf18649b0378bdd7093b3f925137229))
* node test ([64f0b4c](https://github.com/rudderlabs/keydb/commit/64f0b4c7d224da2a6ec8cd86700d82a5e86b04bc))
* release please config ([#55](https://github.com/rudderlabs/keydb/issues/55)) ([f607127](https://github.com/rudderlabs/keydb/commit/f60712796795d1f4212cbd35d21a876327c3fba9))
* release please config ([#57](https://github.com/rudderlabs/keydb/issues/57)) ([16d4c95](https://github.com/rudderlabs/keydb/commit/16d4c95ba4a30f368b90af1c3abffb14e29d8b77))
* resp is nil while retrying ([6856420](https://github.com/rudderlabs/keydb/commit/685642061a9b233368c220ab282924710039bc41))
* scaling operations with load snapshots op ([c927763](https://github.com/rudderlabs/keydb/commit/c9277632751b17640f07fc6569aebd63cd4ce743))
* snapshot handling shouldn't list more than once ([f863cf9](https://github.com/rudderlabs/keydb/commit/f863cf90c10a5a0cf362383cbafc69c0fce34be9))
* tests ([bf6c420](https://github.com/rudderlabs/keydb/commit/bf6c420ac35d5f1b523194fc5422eb216fff72ad))

## [1.2.0](https://github.com/rudderlabs/keydb/compare/keydb-v1.1.0...keydb-v1.2.0) (2025-09-19)


### Features

* add Helm chart and deployment setup for KeyDB Operator ([9da2183](https://github.com/rudderlabs/keydb/commit/9da2183c32eca2a1977cf1c1eafe4abc8ea39f92))
* add more observability and metrics ([#6](https://github.com/rudderlabs/keydb/issues/6)) ([a9c50b1](https://github.com/rudderlabs/keydb/commit/a9c50b1244202567954d57e001f90bc4481cf57c))
* adding stats ([0820dea](https://github.com/rudderlabs/keydb/commit/0820dea032fb8c7cbae12ad7fcb129deb8768171))
* cuckoo filters ([2c9f6c4](https://github.com/rudderlabs/keydb/commit/2c9f6c467cb8d806d1a512c80bd34f17c8bebf5c))
* implement Badger metrics collector ([#5](https://github.com/rudderlabs/keydb/issues/5)) ([a4997b9](https://github.com/rudderlabs/keydb/commit/a4997b9f543477ab27415dd99ac3e7104ea2360b))
* implement GitHub Actions workflows and improve snapshot functionality ([69f2374](https://github.com/rudderlabs/keydb/commit/69f23748d1b552da6d4f59beac13d9e2d160ac9a))
* implement rollback functionality for scaling operations ([#23](https://github.com/rudderlabs/keydb/issues/23)) ([dd119e7](https://github.com/rudderlabs/keydb/commit/dd119e7205cc355ec17a41349969ae284a93d1ff))
* improve keydb write latencies ([#9](https://github.com/rudderlabs/keydb/issues/9)) ([97e8501](https://github.com/rudderlabs/keydb/commit/97e850102b2db088827a215e9362b287b3fc1894))
* load snapshots ([479336c](https://github.com/rudderlabs/keydb/commit/479336c6052290ac0bdee77cbd7b7ca2ecbde96d))
* **node:** add configurable backup folder name and refactor snapshot file path ([#33](https://github.com/rudderlabs/keydb/issues/33)) ([d79c882](https://github.com/rudderlabs/keydb/commit/d79c8829babcf62b714d3181b031810b59976f23))
* operator ([27cbeae](https://github.com/rudderlabs/keydb/commit/27cbeaeffb70f50f8dbcd3bc322e6cf60e7c80b2))


### Bug Fixes

* cache initialization ([a584b7b](https://github.com/rudderlabs/keydb/commit/a584b7b3ab213ca925312a6f92f0e030610fe7ea))
* data races ([d851213](https://github.com/rudderlabs/keydb/commit/d8512138964d3579858c6f7cb7a25cacc7a85f02))
* get/put cluster size changes ([5fcc9d4](https://github.com/rudderlabs/keydb/commit/5fcc9d4374e425b39f84fdceaa09193bb9335dc4))
* growth keys ([3d041fe](https://github.com/rudderlabs/keydb/commit/3d041fef76e80804352901244e6c101b476ae6b8))
* handle context cancellation in snapshot creation ([#39](https://github.com/rudderlabs/keydb/issues/39)) ([9cd22ab](https://github.com/rudderlabs/keydb/commit/9cd22abe3d946e9f049505dc9da701292b31f3da))
* load snapshots ([51740bb](https://github.com/rudderlabs/keydb/commit/51740bb3a5ef346909471479ff7caf20d8b1e452))
* node ([00886a3](https://github.com/rudderlabs/keydb/commit/00886a31baf18649b0378bdd7093b3f925137229))
* node test ([64f0b4c](https://github.com/rudderlabs/keydb/commit/64f0b4c7d224da2a6ec8cd86700d82a5e86b04bc))
* release please config ([#55](https://github.com/rudderlabs/keydb/issues/55)) ([f607127](https://github.com/rudderlabs/keydb/commit/f60712796795d1f4212cbd35d21a876327c3fba9))
* resp is nil while retrying ([6856420](https://github.com/rudderlabs/keydb/commit/685642061a9b233368c220ab282924710039bc41))
* scaling operations with load snapshots op ([c927763](https://github.com/rudderlabs/keydb/commit/c9277632751b17640f07fc6569aebd63cd4ce743))
* snapshot handling shouldn't list more than once ([f863cf9](https://github.com/rudderlabs/keydb/commit/f863cf90c10a5a0cf362383cbafc69c0fce34be9))
* tests ([bf6c420](https://github.com/rudderlabs/keydb/commit/bf6c420ac35d5f1b523194fc5422eb216fff72ad))

## [1.1.0](https://github.com/rudderlabs/keydb/compare/rudder-keydb-v1.0.0...rudder-keydb-v1.1.0) (2025-09-19)


### Features

* add Helm chart and deployment setup for KeyDB Operator ([9da2183](https://github.com/rudderlabs/keydb/commit/9da2183c32eca2a1977cf1c1eafe4abc8ea39f92))
* add more observability and metrics ([#6](https://github.com/rudderlabs/keydb/issues/6)) ([a9c50b1](https://github.com/rudderlabs/keydb/commit/a9c50b1244202567954d57e001f90bc4481cf57c))
* adding stats ([0820dea](https://github.com/rudderlabs/keydb/commit/0820dea032fb8c7cbae12ad7fcb129deb8768171))
* cuckoo filters ([2c9f6c4](https://github.com/rudderlabs/keydb/commit/2c9f6c467cb8d806d1a512c80bd34f17c8bebf5c))
* implement Badger metrics collector ([#5](https://github.com/rudderlabs/keydb/issues/5)) ([a4997b9](https://github.com/rudderlabs/keydb/commit/a4997b9f543477ab27415dd99ac3e7104ea2360b))
* implement GitHub Actions workflows and improve snapshot functionality ([69f2374](https://github.com/rudderlabs/keydb/commit/69f23748d1b552da6d4f59beac13d9e2d160ac9a))
* implement rollback functionality for scaling operations ([#23](https://github.com/rudderlabs/keydb/issues/23)) ([dd119e7](https://github.com/rudderlabs/keydb/commit/dd119e7205cc355ec17a41349969ae284a93d1ff))
* improve keydb write latencies ([#9](https://github.com/rudderlabs/keydb/issues/9)) ([97e8501](https://github.com/rudderlabs/keydb/commit/97e850102b2db088827a215e9362b287b3fc1894))
* load snapshots ([479336c](https://github.com/rudderlabs/keydb/commit/479336c6052290ac0bdee77cbd7b7ca2ecbde96d))
* **node:** add configurable backup folder name and refactor snapshot file path ([#33](https://github.com/rudderlabs/keydb/issues/33)) ([d79c882](https://github.com/rudderlabs/keydb/commit/d79c8829babcf62b714d3181b031810b59976f23))
* operator ([27cbeae](https://github.com/rudderlabs/keydb/commit/27cbeaeffb70f50f8dbcd3bc322e6cf60e7c80b2))


### Bug Fixes

* cache initialization ([a584b7b](https://github.com/rudderlabs/keydb/commit/a584b7b3ab213ca925312a6f92f0e030610fe7ea))
* data races ([d851213](https://github.com/rudderlabs/keydb/commit/d8512138964d3579858c6f7cb7a25cacc7a85f02))
* get/put cluster size changes ([5fcc9d4](https://github.com/rudderlabs/keydb/commit/5fcc9d4374e425b39f84fdceaa09193bb9335dc4))
* growth keys ([3d041fe](https://github.com/rudderlabs/keydb/commit/3d041fef76e80804352901244e6c101b476ae6b8))
* handle context cancellation in snapshot creation ([#39](https://github.com/rudderlabs/keydb/issues/39)) ([9cd22ab](https://github.com/rudderlabs/keydb/commit/9cd22abe3d946e9f049505dc9da701292b31f3da))
* load snapshots ([51740bb](https://github.com/rudderlabs/keydb/commit/51740bb3a5ef346909471479ff7caf20d8b1e452))
* node ([00886a3](https://github.com/rudderlabs/keydb/commit/00886a31baf18649b0378bdd7093b3f925137229))
* node test ([64f0b4c](https://github.com/rudderlabs/keydb/commit/64f0b4c7d224da2a6ec8cd86700d82a5e86b04bc))
* resp is nil while retrying ([6856420](https://github.com/rudderlabs/keydb/commit/685642061a9b233368c220ab282924710039bc41))
* scaling operations with load snapshots op ([c927763](https://github.com/rudderlabs/keydb/commit/c9277632751b17640f07fc6569aebd63cd4ce743))
* snapshot handling shouldn't list more than once ([f863cf9](https://github.com/rudderlabs/keydb/commit/f863cf90c10a5a0cf362383cbafc69c0fce34be9))
* tests ([bf6c420](https://github.com/rudderlabs/keydb/commit/bf6c420ac35d5f1b523194fc5422eb216fff72ad))

## [0.4.3-alpha](https://github.com/rudderlabs/keydb/compare/v0.4.2-alpha...v0.4.3-alpha) (2025-09-18)


### Miscellaneous

* grpc backoffs and keep alives ([#50](https://github.com/rudderlabs/keydb/issues/50)) ([46fc72a](https://github.com/rudderlabs/keydb/commit/46fc72aedd69fdd6fd4d374bd53d80e05abfb3fc))
* improve server start and stop handling ([#51](https://github.com/rudderlabs/keydb/issues/51)) ([e468df6](https://github.com/rudderlabs/keydb/commit/e468df6681eb66484d72df3172151223774fb741))

## [0.4.2-alpha](https://github.com/rudderlabs/keydb/compare/v0.4.1-alpha...v0.4.2-alpha) (2025-09-03)


### Miscellaneous

* improved documentation ([#41](https://github.com/rudderlabs/keydb/issues/41)) ([e4a112a](https://github.com/rudderlabs/keydb/commit/e4a112ad8de79742394f2d062030ad2a8556e2d6))
* infinite exponential retry policy ([#43](https://github.com/rudderlabs/keydb/issues/43)) ([4864918](https://github.com/rudderlabs/keydb/commit/486491836240ee2277fad4109199256b93f4797b))
* load snapshots max concurrency ([#42](https://github.com/rudderlabs/keydb/issues/42)) ([5b15806](https://github.com/rudderlabs/keydb/commit/5b158066be04118a1dd49cbbe346acd1a5735570))

## [0.4.1-alpha](https://github.com/rudderlabs/keydb/compare/v0.4.0-alpha...v0.4.1-alpha) (2025-08-28)


### Bug Fixes

* handle context cancellation in snapshot creation ([#39](https://github.com/rudderlabs/keydb/issues/39)) ([9cd22ab](https://github.com/rudderlabs/keydb/commit/9cd22abe3d946e9f049505dc9da701292b31f3da))


### Miscellaneous

* fixes and log verbosity ([#37](https://github.com/rudderlabs/keydb/issues/37)) ([3590961](https://github.com/rudderlabs/keydb/commit/3590961dea017d4e5ab68d16bff8d4fd44768db2))
* rename operator to scaler ([#35](https://github.com/rudderlabs/keydb/issues/35)) ([a7fe9f4](https://github.com/rudderlabs/keydb/commit/a7fe9f495b773a540ad6d3f71a2cc6a5418dddea))

## [0.4.0-alpha](https://github.com/rudderlabs/keydb/compare/v0.3.1-alpha...v0.4.0-alpha) (2025-08-26)


### Features

* **node:** add configurable backup folder name and refactor snapshot file path ([#33](https://github.com/rudderlabs/keydb/issues/33)) ([d79c882](https://github.com/rudderlabs/keydb/commit/d79c8829babcf62b714d3181b031810b59976f23))

## [0.3.1-alpha](https://github.com/rudderlabs/keydb/compare/v0.3.0-alpha...v0.3.1-alpha) (2025-08-26)


### Miscellaneous

* retry policy ([#29](https://github.com/rudderlabs/keydb/issues/29)) ([0553cc1](https://github.com/rudderlabs/keydb/commit/0553cc1159c3da63bbfe78dac58712ce74044da4))
* skipCreateSnapshots and disable retry policy ([#32](https://github.com/rudderlabs/keydb/issues/32)) ([d3db4d5](https://github.com/rudderlabs/keydb/commit/d3db4d5db464a6ee9ba40f357cde646c00fc4700))

## [0.3.0-alpha](https://github.com/rudderlabs/keydb/compare/v0.2.1-alpha...v0.3.0-alpha) (2025-08-21)


### Features

* implement rollback functionality for scaling operations ([#23](https://github.com/rudderlabs/keydb/issues/23)) ([dd119e7](https://github.com/rudderlabs/keydb/commit/dd119e7205cc355ec17a41349969ae284a93d1ff))


### Miscellaneous

* adding download to hash range movements ([#26](https://github.com/rudderlabs/keydb/issues/26)) ([ea86ab8](https://github.com/rudderlabs/keydb/commit/ea86ab82d0fac6ec1220ded6089287b70488a8bc))
* parallelize in hash range movements ([#24](https://github.com/rudderlabs/keydb/issues/24)) ([e537e74](https://github.com/rudderlabs/keydb/commit/e537e749bce35afe264a57883dcb7eb29a6e6d29))
* update Dockerfile references and image repository for keydb operator ([#25](https://github.com/rudderlabs/keydb/issues/25)) ([e8ae90b](https://github.com/rudderlabs/keydb/commit/e8ae90b1771042898fdb7b73959600fa0b6c28e7))

## [0.2.1-alpha](https://github.com/rudderlabs/keydb/compare/v0.2.0-alpha...v0.2.1-alpha) (2025-08-18)


### Miscellaneous

* snapshots and scaling operations ([#18](https://github.com/rudderlabs/keydb/issues/18)) ([e37dab9](https://github.com/rudderlabs/keydb/commit/e37dab9200e7bc482b57db382c368e47f60840ef))

## [0.2.0-alpha](https://github.com/rudderlabs/keydb/compare/v0.1.0-alpha...v0.2.0-alpha) (2025-07-31)


### Features

* add more observability and metrics ([#6](https://github.com/rudderlabs/keydb/issues/6)) ([a9c50b1](https://github.com/rudderlabs/keydb/commit/a9c50b1244202567954d57e001f90bc4481cf57c))
* adding stats ([0820dea](https://github.com/rudderlabs/keydb/commit/0820dea032fb8c7cbae12ad7fcb129deb8768171))
* implement Badger metrics collector ([#5](https://github.com/rudderlabs/keydb/issues/5)) ([a4997b9](https://github.com/rudderlabs/keydb/commit/a4997b9f543477ab27415dd99ac3e7104ea2360b))
* implement GitHub Actions workflows and improve snapshot functionality ([69f2374](https://github.com/rudderlabs/keydb/commit/69f23748d1b552da6d4f59beac13d9e2d160ac9a))
* improve keydb write latencies ([#9](https://github.com/rudderlabs/keydb/issues/9)) ([97e8501](https://github.com/rudderlabs/keydb/commit/97e850102b2db088827a215e9362b287b3fc1894))
* load snapshots ([479336c](https://github.com/rudderlabs/keydb/commit/479336c6052290ac0bdee77cbd7b7ca2ecbde96d))


### Bug Fixes

* cache initialization ([a584b7b](https://github.com/rudderlabs/keydb/commit/a584b7b3ab213ca925312a6f92f0e030610fe7ea))
* data races ([d851213](https://github.com/rudderlabs/keydb/commit/d8512138964d3579858c6f7cb7a25cacc7a85f02))
* load snapshots ([51740bb](https://github.com/rudderlabs/keydb/commit/51740bb3a5ef346909471479ff7caf20d8b1e452))
* scaling operations with load snapshots op ([c927763](https://github.com/rudderlabs/keydb/commit/c9277632751b17640f07fc6569aebd63cd4ce743))
* snapshot handling shouldn't list more than once ([f863cf9](https://github.com/rudderlabs/keydb/commit/f863cf90c10a5a0cf362383cbafc69c0fce34be9))


### Miscellaneous

* 2nd refactoring of Load ([59af741](https://github.com/rudderlabs/keydb/commit/59af74103198da1cd24f24d5db1ab55295a057cb))
* add nistec.P521Point to .trufflehogignore to prevent false positives ([495bc78](https://github.com/rudderlabs/keydb/commit/495bc78c421bd6aa0628f4eee38ad8411da58f14))
* add regex pattern to .truffleignore for nistec.P521Point ([142746f](https://github.com/rudderlabs/keydb/commit/142746f9aea68eb65a4bae513b95cc93f5ba700f))
* adding !cache to .dockerignore ([5ad5ac6](https://github.com/rudderlabs/keydb/commit/5ad5ac6314d098107c6676754e4ccbd7a945143b))
* adding comment ([b552c8c](https://github.com/rudderlabs/keydb/commit/b552c8c34bf15c3c6f014f403e379b739b580d63))
* adding comment ([7980719](https://github.com/rudderlabs/keydb/commit/79807195d01c78e37caddba5fc499bc3add6e059))
* adding comment ([c05352b](https://github.com/rudderlabs/keydb/commit/c05352b73b2bce9d48068f33bfd959a8e4001333))
* adding junie guidelines ([f13e91c](https://github.com/rudderlabs/keydb/commit/f13e91cd5de22c9f40c50f3a60cd2786ae665783))
* adding TODO ([773f780](https://github.com/rudderlabs/keydb/commit/773f780d1193ab085e160c9cdbe2073565dd928b))
* adding TODO ([93403fd](https://github.com/rudderlabs/keydb/commit/93403fda6881c98708c428d778fc7861f71f0374))
* adjust database configuration parameters ([#8](https://github.com/rudderlabs/keydb/issues/8)) ([ab68d28](https://github.com/rudderlabs/keydb/commit/ab68d284988ce63155f51a8220862b6a2d8e3d47))
* badger nop logger ([7ed7a10](https://github.com/rudderlabs/keydb/commit/7ed7a10962c0fdcc4e6c092c8e1a7b644d579a8b))
* better log message ([c440a48](https://github.com/rudderlabs/keydb/commit/c440a482ecc3106bc9f9b6d71913269985390f0f))
* better shutdown ([541bd30](https://github.com/rudderlabs/keydb/commit/541bd30a75217f9b87920431fd2f0d21fe3b02b0))
* clean up ([af50b32](https://github.com/rudderlabs/keydb/commit/af50b320786bdd4be089052ed645da978bf12eee))
* cleaning up ([2f3e213](https://github.com/rudderlabs/keydb/commit/2f3e21376db385de64243cd4243fa958f1b9c685))
* custom backup draft ([81d37f9](https://github.com/rudderlabs/keydb/commit/81d37f9afad60e4f934e18a27dabe9119d5872aa))
* disabling automatic snapshot creation ([485f7f4](https://github.com/rudderlabs/keydb/commit/485f7f42c6c77fd4f0309fe756ec70c96be8b050))
* docs ([7e567a7](https://github.com/rudderlabs/keydb/commit/7e567a7d6ebffbffcc88df555ed38c298d48fff2))
* docs + rename ([7369ebc](https://github.com/rudderlabs/keydb/commit/7369ebcde92208db1e88b84be02388ac97cd9427))
* first refactoring of Load ([094ddb3](https://github.com/rudderlabs/keydb/commit/094ddb3e6687699a7c393d45812cac73039e1053))
* garbage collection ([64bb247](https://github.com/rudderlabs/keydb/commit/64bb247e3261c6adabcf1e43b0f7892c717a3669))
* increasing op api timeouts ([03c2320](https://github.com/rudderlabs/keydb/commit/03c232073ddecd4ebe548ce288f671bebb43029f))
* line length linter ([#14](https://github.com/rudderlabs/keydb/issues/14)) ([b36dca0](https://github.com/rudderlabs/keydb/commit/b36dca0a3203ee5e6e7611eda662d7c033fa8415))
* line length linting ([#15](https://github.com/rudderlabs/keydb/issues/15)) ([c7fe5c8](https://github.com/rudderlabs/keydb/commit/c7fe5c80777236a03efd3c4df8d780600b0d17f5))
* log entries ([4616c96](https://github.com/rudderlabs/keydb/commit/4616c96f6858e8dde47c8cf4eae0cab5002d91b0))
* making hash ranges download optional ([4b6157b](https://github.com/rudderlabs/keydb/commit/4b6157bf77c05d858913066381802d7d904cbeb0))
* moving debug methods to the bottom ([ccaae2a](https://github.com/rudderlabs/keydb/commit/ccaae2a07bfb5bcd8cef41476a3ebf0a539136a8))
* new info log ([ba29672](https://github.com/rudderlabs/keydb/commit/ba29672b215b02d9f509bb7d7bff63b4d0c583af))
* node refactoring with single badger ([5da7d16](https://github.com/rudderlabs/keydb/commit/5da7d1666f7a245f665c07ebecc171241c27ce68))
* pushing some fixes ([993eb45](https://github.com/rudderlabs/keydb/commit/993eb4585b6817fba8dde880e0b9f9791b29b162))
* reducing number of compactors ([3fbb38f](https://github.com/rudderlabs/keydb/commit/3fbb38fb34d4b5e628e895ac56f9ab062ae812d3))
* reducing the number of compactors ([2a162d4](https://github.com/rudderlabs/keydb/commit/2a162d4144927d01a0262d68008345ee377992e8))
* reformat ([57fb0e9](https://github.com/rudderlabs/keydb/commit/57fb0e912e6e089ae1c49ded3ef820630f8553e5))
* removing cachettl ([81e69a5](https://github.com/rudderlabs/keydb/commit/81e69a5b5f811a943b44aa13642b5cc4d6f3daa9))
* removing expired keys deletion ([1db7ce5](https://github.com/rudderlabs/keydb/commit/1db7ce5ddead7cc458edb2e6660c5e28cf6b0e3f))
* removing TODO ([6ca53f1](https://github.com/rudderlabs/keydb/commit/6ca53f16dc7916aec6195c6a18b248b4253d42b5))
* removing unnecessary factory ([77cc0d9](https://github.com/rudderlabs/keydb/commit/77cc0d9024cba55d9a5f32e3eef9b1da58a36cd8))
* renamed /snapshot ([a1e6f2b](https://github.com/rudderlabs/keydb/commit/a1e6f2bb87cc5c81ced24f22b79668cae6ec6200))
* renaming to default timeout ([e72863d](https://github.com/rudderlabs/keydb/commit/e72863d21d4ffe48f90a255ff4b56b1f225dfa35))
* snapshots ([bc06b2d](https://github.com/rudderlabs/keydb/commit/bc06b2df088e1412d35b6366b0033bb4ad9c4056))
* snapshots compression ([0d4f2ac](https://github.com/rudderlabs/keydb/commit/0d4f2ac24426158e673381a330bc8a8c930834d2))
* update .truffleignore to exclude binary files and blobs directory ([ccb6f6e](https://github.com/rudderlabs/keydb/commit/ccb6f6e204d6c5e80b762e84d1b25f39a04fd5b4))
* update .truffleignore to exclude keydb directory ([7e0fbbf](https://github.com/rudderlabs/keydb/commit/7e0fbbfb606d08295d53eefd50202cf90830ff76))
* update .truffleignore to exclude keydb directory ([3d3d90c](https://github.com/rudderlabs/keydb/commit/3d3d90c13654344ef6f6b96a3f6a508ed27819af))
* update .truffleignore to exclude vendor directory and Go module files ([8eb498c](https://github.com/rudderlabs/keydb/commit/8eb498c1c4f609d87a05d8b0753bdc86304f971b))
* update .truffleignore to ignore broader cryptographic patterns ([5aedb40](https://github.com/rudderlabs/keydb/commit/5aedb40b5c4d7d0a844473f71a569fca1d1b0b64))
* update .truffleignore to include vendor directory ([5393b8b](https://github.com/rudderlabs/keydb/commit/5393b8be92ca18ce763b63861dfde057663570b1))
* update Alpine version to 3.22 in Dockerfiles ([e532003](https://github.com/rudderlabs/keydb/commit/e532003f1e8fb952432e15dfbd3d89dc31824816))
* update log fields to use strings instead of durations ([19adfe8](https://github.com/rudderlabs/keydb/commit/19adfe880742074fbfab94c4f2e092bcd940b7ba))
* updating AI guidelines ([39524c0](https://github.com/rudderlabs/keydb/commit/39524c0f32b708d2ac23d24724a4a1afa4ccbe54))
* using 1 compactor ([671edb7](https://github.com/rudderlabs/keydb/commit/671edb77d38af28856b68f86d8161f0b7334182d))
* using 2 compactors ([d9c3f2c](https://github.com/rudderlabs/keydb/commit/d9c3f2ce26583ad575246e2e4035df2c582d4d03))
