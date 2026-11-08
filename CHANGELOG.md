# Changelog

## [0.7.0](https://github.com/LevelFourAB/syncplane/compare/v0.6.0...v0.7.0) (2026-11-08)


### Features

* **engine:** Log and count workspaces a Subscribe drops ([ab86334](https://github.com/LevelFourAB/syncplane/commit/ab86334d613a24662794b8786b97ef7631ab4d82))
* Provide approximate bootstrap and sync progress ([ae8cfa1](https://github.com/LevelFourAB/syncplane/commit/ae8cfa1e03a478ae4708f19e69671d5a5ed78933))


### Bug Fixes

* **client:** Analyze newer Unicode letters as the server does ([85f3e23](https://github.com/LevelFourAB/syncplane/commit/85f3e23ec13d4d1e73920cc9099411d286d26493))
* **client:** Close a tab at its next write after a peer purge ([398b33c](https://github.com/LevelFourAB/syncplane/commit/398b33c365e8fa11d4b8437701a4a7577cc4bd41))
* **client:** Copy values in the memory store as IndexedDB does ([93e5831](https://github.com/LevelFourAB/syncplane/commit/93e5831017afc4311db4aff2144bd289a7b683fc))
* **client:** Drop a newer tab's edit once an old tab acks it ([d5bbc90](https://github.com/LevelFourAB/syncplane/commit/d5bbc90975d1fd2c579d8dcb895e9527e2181d6e))
* **client:** Drop no-op collection patches instead of the batch ([a00970b](https://github.com/LevelFourAB/syncplane/commit/a00970bb09cada4b4e6d5e45cb60c6f16d2f10e5))
* **client:** Keep a created row when a later PUT is refused ([edaec40](https://github.com/LevelFourAB/syncplane/commit/edaec405e6bf02935ac4a7fd2a29d6c57e406015))
* **client:** Keep a later PUT apart from text it did not carry ([2910668](https://github.com/LevelFourAB/syncplane/commit/2910668c59a4c1dc3e9092a9b40f369ad5c160bd))
* **client:** Keep a new write when two tabs close an id gap ([3553723](https://github.com/LevelFourAB/syncplane/commit/35537235457fddede563e0be485abc393c2bb17d))
* **client:** Keep a newer tab's edit when an old tab rebases ([a47f284](https://github.com/LevelFourAB/syncplane/commit/a47f2845cff6b17ee8439da6e44a602459e18d34))
* **client:** Keep a purged workspace empty when a pull lands late ([dabdfeb](https://github.com/LevelFourAB/syncplane/commit/dabdfeb22dee9097e4562710225b3574c3de9240))
* **client:** Keep an edit acked during a resumed re-bootstrap ([d1d6d3a](https://github.com/LevelFourAB/syncplane/commit/d1d6d3aa5f9a2c7d1f3a2eab50482202e1590697))
* **client:** Keep newer-build fields on writes compaction folds ([e10e4d8](https://github.com/LevelFourAB/syncplane/commit/e10e4d835d4edbfee286171deb738621f17fe4df))
* **client:** Keep other field edits out of a typing undo step ([7ad8411](https://github.com/LevelFourAB/syncplane/commit/7ad8411db7d193a12e7a9cd00be704c2010a2eb7))
* **client:** Keep pushing when queue compaction fails ([f910a9f](https://github.com/LevelFourAB/syncplane/commit/f910a9f5698a165f4cea0449732c2fdb3b7694a2))
* **client:** Keep queued writes apart when folding passes limits ([ba368a5](https://github.com/LevelFourAB/syncplane/commit/ba368a58badc5c21cd61291bca9acb06150e2d70))
* **client:** Keep queued writes in order around an access change ([e323588](https://github.com/LevelFourAB/syncplane/commit/e323588e87644e19db02d6e890a93e274f2ba036))
* **client:** Keep staged rows another tab adopted ([29d1379](https://github.com/LevelFourAB/syncplane/commit/29d1379dcf660cdffdac71b7afefcc2a05cc5997))
* **client:** Keep syncing when a text field is not declared ([bfc0faa](https://github.com/LevelFourAB/syncplane/commit/bfc0faa1afed00680c69826a091cec49eb95e1fb))
* **client:** Keep tabs of other builds on one sync scope ([aad8f36](https://github.com/LevelFourAB/syncplane/commit/aad8f36f60bf2119e99e7f965919cb3495b6c846))
* **client:** Keep writes made on a store restored from backup ([9e1baa5](https://github.com/LevelFourAB/syncplane/commit/9e1baa5e324915daafa35237ae8fa5cc423187fe))
* **client:** Let update() change more than 64 list items ([2f308d0](https://github.com/LevelFourAB/syncplane/commit/2f308d0c7b71653c99d3d969ba7dd2a2ea726c9a))
* **client:** Let update() change one field of a row over 1 MiB ([6bf3b33](https://github.com/LevelFourAB/syncplane/commit/6bf3b332739c055cabe8d742c278aa92062aebe5))
* **client:** Raise the read floor after a lost push response ([665a2a8](https://github.com/LevelFourAB/syncplane/commit/665a2a8807f310e5cea9794bf01198baa9862dcb))
* **client:** Rebuild local indexes another build rewrote ([3f89699](https://github.com/LevelFourAB/syncplane/commit/3f896996c7296f3e7c684efe2810e08d70e9c579))
* **client:** Refresh live queries on back/forward restore ([ea409d3](https://github.com/LevelFourAB/syncplane/commit/ea409d3a1ca0cb3bfb16d9a1a5f316a7be331d74))
* **client:** Refuse a superseded tab's bootstrap rows ([2187d01](https://github.com/LevelFourAB/syncplane/commit/2187d01c647805a1d32c1601b8a7bb1bad4f98cd))
* **client:** Refuse a transact() over the push limit up front ([e213568](https://github.com/LevelFourAB/syncplane/commit/e213568beb4f1e714651366fbf7cc6db01c2bdaf))
* **client:** Report a text edit too large to re-send as a conflict ([10af60a](https://github.com/LevelFourAB/syncplane/commit/10af60af1c2dcb97a9ffdc1d25ccfcc608bf6dde))
* **client:** Return no order keys for a zero or negative count ([e94bab6](https://github.com/LevelFourAB/syncplane/commit/e94bab6b5a3db643878544b1f455f9e62bec907c))
* **client:** Show others' newer values while push is blocked ([5d126db](https://github.com/LevelFourAB/syncplane/commit/5d126dbc8d631f4b96973b6a395a9167e5e7b5ef))
* **client:** Show true sync progress after a peer resync ([c10c210](https://github.com/LevelFourAB/syncplane/commit/c10c21018376a526bb157a772b3e17052cece135))
* **client:** Split pushes that are too large for one request ([4b59220](https://github.com/LevelFourAB/syncplane/commit/4b59220acf9018b8836c7f0a3f7c287d37711db4))
* **client:** Stop old tabs from wiping a newer build's store ([59a83c3](https://github.com/LevelFourAB/syncplane/commit/59a83c38675a1750eead76a685de3873ede33585))
* **client:** Store a re-created text row after a refused delete ([4f68d33](https://github.com/LevelFourAB/syncplane/commit/4f68d33c1caf5775b18ae740cbf649608f38acfd))
* **client:** Time out sync requests that never answer ([a69e522](https://github.com/LevelFourAB/syncplane/commit/a69e52261d5cf98f63f83aa23077d59c00cffb5b))
* **engine:** Accept writes that keep a value too long to index ([bea9f56](https://github.com/LevelFourAB/syncplane/commit/bea9f56d18698093be3a097be2f641d08dbb5c69))
* **engine:** Apply a guarded write over a newer origin change ([29a3d7b](https://github.com/LevelFourAB/syncplane/commit/29a3d7b0991b5e46675d0e26888abf93f951a883))
* **engine:** Confirm forwarded writes that change no visible row ([573db6d](https://github.com/LevelFourAB/syncplane/commit/573db6d5c28f31baa2d318e7891861ff21255e8c))
* **engine:** Deliver every effect after a workspace restore ([dbd6fad](https://github.com/LevelFourAB/syncplane/commit/dbd6fad0aed894698fb43e96673cfabdb1ce7d5f))
* **engine:** Detect a backup restore after GC on idle workspaces ([233a965](https://github.com/LevelFourAB/syncplane/commit/233a9654cedd7ae93bccb4a38d5ac292a038f5bf))
* **engine:** Forward large text edits as fragments to the origin ([39c40ff](https://github.com/LevelFourAB/syncplane/commit/39c40ff76cff48ab2e45063cdc6f3e6b677a6190))
* **engine:** Give effects a unique write-back key per change ([9f914ce](https://github.com/LevelFourAB/syncplane/commit/9f914ceb069dcfea69bc65609db93976bef1992a))
* **engine:** Keep a deleted workspace's rows out of a snapshot ([ff0a80a](https://github.com/LevelFourAB/syncplane/commit/ff0a80ae8f0138208deca2af280ae310b70442d3))
* **engine:** Keep a stale effects pass from moving a consumer back ([8e5b324](https://github.com/LevelFourAB/syncplane/commit/8e5b3241378ba86a8bf106714e3c4130ddd521b9))
* **engine:** Keep blobs of a workspace restored during deletion ([176548e](https://github.com/LevelFourAB/syncplane/commit/176548e0ec3e9b7831005f05d228f71d0aeff2e7))
* **engine:** Keep effects running past an undecodable row ([8106db3](https://github.com/LevelFourAB/syncplane/commit/8106db315c1cfdee0b473bffc46cca1a9cab0d95))
* **engine:** Keep late rejections until a pull delivers them ([9756c76](https://github.com/LevelFourAB/syncplane/commit/9756c76132d02b93bf2477b904eb6d28e63d86cc))
* **engine:** Keep later writes over a re-queued text write ([8e9ce09](https://github.com/LevelFourAB/syncplane/commit/8e9ce095fa509e5a1696f2f9df32b159acc2f9f2))
* **engine:** Keep partial-sync clients converged across pages ([f335c00](https://github.com/LevelFourAB/syncplane/commit/f335c00c8430c3cd4a8440c2963fa3c1279f22e7))
* **engine:** Keep stored undeclared fields on PUT via old nodes ([b2887d7](https://github.com/LevelFourAB/syncplane/commit/b2887d7b9b3f39db2e7c0f9bbaa3ff1833dc8286))
* **engine:** Keep stream cursors through a cursor-state outage ([11a0629](https://github.com/LevelFourAB/syncplane/commit/11a0629a0f901b38ab95a3847cd5543cd398e273))
* **engine:** Keep undeclared fields from clients of older nodes ([f32eea5](https://github.com/LevelFourAB/syncplane/commit/f32eea5db029b4a7a35d02c7c6c7c7707b3da350))
* **engine:** Push again writes lost by an origin restore ([751966a](https://github.com/LevelFourAB/syncplane/commit/751966a579f76f1f9bc55a68933339a49b0c6899))
* **engine:** Read from replicas again after a workspace restore ([6371f34](https://github.com/LevelFourAB/syncplane/commit/6371f3495b853723f302f889cd148cf0a2d8438b))
* **engine:** Refuse a negative client retention at startup ([07baf59](https://github.com/LevelFourAB/syncplane/commit/07baf59ef7f80538cc4496ce023d5ef5be90d68b))
* **engine:** Refuse local writes over the origin's value limits ([a59f618](https://github.com/LevelFourAB/syncplane/commit/a59f618336bbf3d331bbbb742537f7ec666b90a6))
* **engine:** Refuse local writes too large for one origin push ([bf57a0f](https://github.com/LevelFourAB/syncplane/commit/bf57a0f4b84d0b0f4b718a698d3f20bbe548e422))
* **engine:** Report storage failures in mutators as internal ([a0750db](https://github.com/LevelFourAB/syncplane/commit/a0750dbcb9a48be020e8a3b691dae7073f3e7cd3))
* **engine:** Report stored pending writes when a link starts ([43dab35](https://github.com/LevelFourAB/syncplane/commit/43dab35176bfb212443a67298e0824f011b52619))
* **engine:** Report writes lost to a shared link client id ([ccc5259](https://github.com/LevelFourAB/syncplane/commit/ccc525959265b8869d4c4d0fcf6283ee07dd79a0))
* **engine:** Resend window members that return between pages ([038c140](https://github.com/LevelFourAB/syncplane/commit/038c1405d2e115402d2d32f8b1d30af344494009))
* **engine:** Reset connector shadows after a workspace restore ([cbacbd1](https://github.com/LevelFourAB/syncplane/commit/cbacbd1f7a4356b989402108159ed3d70e1417be))
* **engine:** Stop a Bootstrap stream when access is revoked ([44234f4](https://github.com/LevelFourAB/syncplane/commit/44234f4cb625fb4b59dfceb302e0ded4b7bf1274))
* **engine:** Stop failing writes that commit as they cancel ([ccb093c](https://github.com/LevelFourAB/syncplane/commit/ccb093c151aae19714f090514dfb0614fed4e384))
* **engine:** Stop re-bootstrapping a page that ended on a group ([c0516d2](https://github.com/LevelFourAB/syncplane/commit/c0516d2b2514df666bd4faeca0c447612a4d9a49))
* **engine:** Stop re-bootstrapping on an undeclared text field ([adff3e9](https://github.com/LevelFourAB/syncplane/commit/adff3e9a02dc2fda4502c5553589300438d1fd8c))
* **engine:** Stop reusing write ids when a replica lags ([c105954](https://github.com/LevelFourAB/syncplane/commit/c1059545a4f596bb957c1c7aa196655bfd3f4541))
* **engine:** Stop serving a deleted workspace from a replica ([a3a81b5](https://github.com/LevelFourAB/syncplane/commit/a3a81b5b5512b957304fe8e0cee3c7c310fe8a64))
* **engine:** Stop text compaction when the server closes ([4040da1](https://github.com/LevelFourAB/syncplane/commit/4040da193bb57c0281542d5a6c3fa8a4496ae1de))
* **engine:** Stop trusting indexes another schema left stale ([437c141](https://github.com/LevelFourAB/syncplane/commit/437c141025128956d683f6ce07e6e5d227950681))
* **engine:** Upload blobs of rows that left the link's scope ([677f0ea](https://github.com/LevelFourAB/syncplane/commit/677f0ea992c998ab6809dacd1853570789d4aea2))
* Keep a schema-skew block or resync across stream pages ([f1cfcb5](https://github.com/LevelFourAB/syncplane/commit/f1cfcb5967dd46d99385581f4ca5f57ca64ea757))
* Keep set edits on float, double and bytes lists ([4149e9a](https://github.com/LevelFourAB/syncplane/commit/4149e9a9720c2a8ae349ff9c6d3c8d2915b6624f))
* Refuse text marks whose key or value cannot encode ([bb875b4](https://github.com/LevelFourAB/syncplane/commit/bb875b4aee28bb3d652c9f551055ab452ec8c303))
* Refuse the map key "__proto__" instead of losing it ([5ebde47](https://github.com/LevelFourAB/syncplane/commit/5ebde472525461d00c57aaf0940e5fc76c167176))


### Performance Improvements

* **engine:** Stop idle effects passes from writing positions ([da30bb4](https://github.com/LevelFourAB/syncplane/commit/da30bb49551eb29d56ae3acc2c6e9520339c882f))

## [0.6.0](https://github.com/LevelFourAB/syncplane/compare/v0.5.0...v0.6.0) (2026-11-03)


### Features

* **engine:** Support engine syncing with an origin server ([61d5093](https://github.com/LevelFourAB/syncplane/commit/61d50936eb9ee35d4df0d2027ec177cc78ff07ed))

## [0.5.0](https://github.com/LevelFourAB/syncplane/compare/v0.4.1...v0.5.0) (2026-11-01)


### Features

* **client:** Ability to save an undo scope for later use ([f656d07](https://github.com/LevelFourAB/syncplane/commit/f656d07dd2b2c4a65e143ff174504f0104618d1a))
* **client:** Let React tabs and pages keep their undo history after they unmount ([71f208c](https://github.com/LevelFourAB/syncplane/commit/71f208c84a972d659864e3467d539524de5f6768))


### Bug Fixes

* **client:** Download a cached blob again when its stored bytes do not match its digest ([3d0b4c7](https://github.com/LevelFourAB/syncplane/commit/3d0b4c7e81109369360411452f9a629abee29c60))

## [0.4.1](https://github.com/LevelFourAB/syncplane/compare/v0.4.0...v0.4.1) (2026-10-31)


### Bug Fixes

* **client:** Stop endless pulls when the stored cursor is behind the rows already applied ([7643660](https://github.com/LevelFourAB/syncplane/commit/7643660a9295b7e644d2ed75929c2a2b75cc9b9c))

## [0.4.0](https://github.com/LevelFourAB/syncplane/compare/v0.3.0...v0.4.0) (2026-10-31)


### Features

* Blob storage to support attaching binary data to rows ([737afcb](https://github.com/LevelFourAB/syncplane/commit/737afcbfed85740c2ed686127681bf2bc8878d7d))
* **client:** Let a text patch name the document the editor last saw ([dec4715](https://github.com/LevelFourAB/syncplane/commit/dec4715b87147d3a388afd46f77a14a330c90fe0))
* **client:** Let apps await setSubscriptions() until the new scope is local ([c2efea9](https://github.com/LevelFourAB/syncplane/commit/c2efea988a23273c6fb4a718aa589d350d20a3c2))
* **engine:** Let server-side code make list order keys that match the client ([4783830](https://github.com/LevelFourAB/syncplane/commit/4783830012597db16ea97efdfa5725a831507649))
* **engine:** Let servers delete a workspace ([ae5f336](https://github.com/LevelFourAB/syncplane/commit/ae5f336ee7c4db43f2ad469acb6e1aa6528eb374))
* Let clients sync through a message channel instead of HTTP ([e30b89c](https://github.com/LevelFourAB/syncplane/commit/e30b89c17a8492e91060e19ec1898224e74084b8))


### Bug Fixes

* Return the same rows from an index as from a scan when ordered rows tie ([c9cd20e](https://github.com/LevelFourAB/syncplane/commit/c9cd20ebf32c3a792af38302abad351905b88584))


### Performance Improvements

* **client:** Cut IndexedDB storage for full-text search by about a third ([e7ca6d4](https://github.com/LevelFourAB/syncplane/commit/e7ca6d475e500fd59d0c7e29d9f4129a08416526))
* **client:** Key search records by a short scope fingerprint instead of entity and workspace ([10523bc](https://github.com/LevelFourAB/syncplane/commit/10523bcf1e416d8db5e0410ece1d297d8a992d9c))
* **client:** Store full-text search postings as packed term lists ([12a1b41](https://github.com/LevelFourAB/syncplane/commit/12a1b414894bd8e323340aba2a1a9944f6fa3149))
* **engine:** Cut SQLite index storage by a third for text search ([f994128](https://github.com/LevelFourAB/syncplane/commit/f9941283e356fe34d34f34e96a798d479cc8ce2b))
* **engine:** Cut SQLite index storage for text-heavy rows by a fifth ([3af80e4](https://github.com/LevelFourAB/syncplane/commit/3af80e44dab8084fc7f88d9960bc3db709d8b3cc))
* **engine:** Make reference indexes sparse to cut storage for empty refs ([8111241](https://github.com/LevelFourAB/syncplane/commit/8111241aab95f93909aeb9caf72d9bf7039cd6fd))
* **engine:** Shrink index storage with short scope ids and per-row entry lists ([d34c34c](https://github.com/LevelFourAB/syncplane/commit/d34c34c5c9208f3d3a2a920aecb30d9e80e6bae3))
* **engine:** Shrink the SQLite search index by dropping row keys from search entries ([3a88b04](https://github.com/LevelFourAB/syncplane/commit/3a88b0419f7f0711b2aac6947bc3e27f9523d6db))
* **query:** Serve a sort against an index's direction from the index ([e8f8671](https://github.com/LevelFourAB/syncplane/commit/e8f8671d63cf7bd48f0581da40e29bc4ef325a5c))

## [0.3.0](https://github.com/LevelFourAB/syncplane/compare/v0.2.0...v0.3.0) (2026-10-29)


### Features

* **engine:** Add typed, iterator-based reads and mutators ([35a5f79](https://github.com/LevelFourAB/syncplane/commit/35a5f79152fce6c83b1a7801db19c1c21941e80a))
* **engine:** Detect restored stores and reset clients ahead of the backup ([81a60cf](https://github.com/LevelFourAB/syncplane/commit/81a60cf49f58334715120c22221b8e99072dc7d1))


### Bug Fixes

* **client:** Keep a write another tab already pushed instead of dropping it as poison ([a35d502](https://github.com/LevelFourAB/syncplane/commit/a35d50204baa2a9748d99ce41751e70e00b0a320))
* **client:** Keep a write made during a format upgrade instead of wiping it ([41db80c](https://github.com/LevelFourAB/syncplane/commit/41db80c04b21b0ee5c2d88413917a9dca79947d1))
* **client:** Keep new text typed right after clearing a compacted text field ([1bc3956](https://github.com/LevelFourAB/syncplane/commit/1bc39565e978f7b678eeb5aaa7440db68063ee0e))
* **client:** No longer lose queued text edits when a pull brings a compacted document ([29c3dd9](https://github.com/LevelFourAB/syncplane/commit/29c3dd953f08a97c27454d2542b8352f8c8abbee))
* **client:** Report a rejected edit when another tab syncs before the lost push is re-sent ([df280ff](https://github.com/LevelFourAB/syncplane/commit/df280ff493500ec19f29556dbfb8a4f6513ee810))
* **client:** Report writes the server rejects during a local format upgrade ([deca49f](https://github.com/LevelFourAB/syncplane/commit/deca49fd30cd8dbf56b4ff254c658a4cfb0307eb))
* **client:** Stop compaction recovery from deleting text typed by other users ([93e73e2](https://github.com/LevelFourAB/syncplane/commit/93e73e2f938b8f9c7f558b06fbe73b72d53c4382))
* **client:** Stop path-style row keys from sharing optimistic text edits ([f94b1b9](https://github.com/LevelFourAB/syncplane/commit/f94b1b9a25cde03cc61347042a43deab59af2f26))
* **client:** Store recovered text edits until they are re-applied ([9fc6e5e](https://github.com/LevelFourAB/syncplane/commit/9fc6e5e1138de4b406a61c4db7dc29da46284519))
* **client:** Sync rows whose int64 id is written as a string like "007" or "0x10" ([68aa054](https://github.com/LevelFourAB/syncplane/commit/68aa0546254b04e78f80921ff39ca3b0cf993fac))
* **client:** Undo of a mark no longer removes formatting a peer applied on top of it ([c156a1e](https://github.com/LevelFourAB/syncplane/commit/c156a1e7363c4e78c906934a075e6252916cf845))
* **engine:** Accept new text typed into a compacted text field after it was cleared ([386abb4](https://github.com/LevelFourAB/syncplane/commit/386abb494641444ef27678736e0367d8e44d1f70))
* **engine:** Always keep the rejections of a client's latest push ([43ab390](https://github.com/LevelFourAB/syncplane/commit/43ab390ae53a63b6a3e7d560388f0123d27afbb3))
* **engine:** Keep a push batch going when a text document sits under a \u0000 key ([ef91e4f](https://github.com/LevelFourAB/syncplane/commit/ef91e4f460e8d61c8f6a0414dba7bdd106d665db))
* **engine:** Keep an interrupted bootstrap from leaving rows outside the window or closure ([364ef14](https://github.com/LevelFourAB/syncplane/commit/364ef14f6ff2978f06637b2875dbc9919b4fdc92))
* **engine:** Keep indexed queries finding rows after a PATCH into an opaque field or reference ([eaaad05](https://github.com/LevelFourAB/syncplane/commit/eaaad05395c05b85b2a28cf9b3fd56a9120c99c5))
* **engine:** Keep refused in-process writes from leaking text into readers copies ([0a0dd53](https://github.com/LevelFourAB/syncplane/commit/0a0dd5352ff339aa7954eda6ec1ecc8a86e3178e))
* **engine:** Keep rows hidden by the read filter off clients that bootstrap during a write ([cc669e1](https://github.com/LevelFourAB/syncplane/commit/cc669e14a636775bf7990231d37294fd70db6ce3))
* **engine:** Keep windowed rows in sync when a scope change follows a window move ([9c73083](https://github.com/LevelFourAB/syncplane/commit/9c7308396656f47b1558b6dca6ed65291c9f8a41))
* **engine:** No longer drop a write when a new client's first push sends ids out of order ([c32eed4](https://github.com/LevelFourAB/syncplane/commit/c32eed4094e0b1fb9a4d3a18ea44341dcb6f4f7f))
* **engine:** Refuse idempotency keys that are not valid UTF-8 or hold \u0000 on every store ([515b802](https://github.com/LevelFourAB/syncplane/commit/515b8027d2ba20f221ad952f8c8ebef21f26112f))
* **engine:** Reject duplicate keyed-list keys with invalid_argument instead of storing them ([8fb974a](https://github.com/LevelFourAB/syncplane/commit/8fb974ad07b4e81ff5c741db0c4bfbe6792407e6))
* **engine:** Stop paginated pulls from leaving stale rows outside a window or closure ([be447a6](https://github.com/LevelFourAB/syncplane/commit/be447a6bf1dc55ac40160c60ed153fba4c85e4b2))
* **engine:** Stop the change feed from skipping writes after an access move ([a4de84a](https://github.com/LevelFourAB/syncplane/commit/a4de84ad885309e82dc02b2bc112e923b720cef2))
* Keep a list element or map entry when a patch clears one of its fields ([d3a6ce6](https://github.com/LevelFourAB/syncplane/commit/d3a6ce6acf2a8af34938b5fbb9e3747f2693e4c5))
* Keep queued writes through a server outage instead of dropping them as poison ([eb51312](https://github.com/LevelFourAB/syncplane/commit/eb51312ca06bd894056d6ab925b7724f4dac1332))
* Keep stored text on PUTs without a document ([e6fae74](https://github.com/LevelFourAB/syncplane/commit/e6fae744235092661a7fd3873379f0de6c383ce9))
* Merge text in either order to the same bytes when a peer entry has counter 0 ([704969c](https://github.com/LevelFourAB/syncplane/commit/704969cac34ef745204b84e20b6ad1d16fd35fe2))

## [0.2.0](https://github.com/LevelFourAB/syncplane/compare/v0.1.0...v0.2.0) (2026-10-24)


### Features

* Add WriteFilter for controlling writes based on current data ([11281b2](https://github.com/LevelFourAB/syncplane/commit/11281b26a9483c59d000419368d5f065e7d4874a))
* Declare access permissions, roles and access parents within schema ([56165df](https://github.com/LevelFourAB/syncplane/commit/56165df2cbab14397686108add5d5d4aed07b0ab))


### Bug Fixes

* **client:** Keep a delete and re-create as separate writes ([992e717](https://github.com/LevelFourAB/syncplane/commit/992e717d96b820ef38ba2782021f7ff357825726))
* **client:** Keep accepted edits when the server refuses another edit to the same row ([fc1caf8](https://github.com/LevelFourAB/syncplane/commit/fc1caf84eb55638c2a22623c9c4ecf9917ab2ba0))
* **client:** Keep an unconfirmed edit queued when the client re-downloads a workspace ([c2f7afe](https://github.com/LevelFourAB/syncplane/commit/c2f7afebe4dccc5fc7fe979a8b6a4cb04ae57c5d))
* **client:** Make useGroupCount and useGroupRows respect the workspaceId option ([c94c757](https://github.com/LevelFourAB/syncplane/commit/c94c757600d34e78916a6a7b7cc938ee843d1288))
* **client:** Refuse map keys outside the key type's range in local queries ([6d99d52](https://github.com/LevelFourAB/syncplane/commit/6d99d520c6c473db62181704aaafb5b96a869f5f))
* **client:** Stop text fields that start with U+FEFF from becoming unreadable ([496943b](https://github.com/LevelFourAB/syncplane/commit/496943b514209bbe45385161fb1bb73a313727db))
* **engine:** Accept compiled messages after a schema is registered from a descriptor set ([354065a](https://github.com/LevelFourAB/syncplane/commit/354065a46d030b3049308801333af1730fa84fce))
* **engine:** Answer out-of-range mutation ids with invalid_argument ([cf1e439](https://github.com/LevelFourAB/syncplane/commit/cf1e43957c7900bc336c70bc286e2352a51df3eb))
* **engine:** Keep idempotency keys per caller so one user never receives another's recorded result ([5f9bc9b](https://github.com/LevelFourAB/syncplane/commit/5f9bc9b6f00a84d3b2706cb0a2dae3652d6483bb))
* **engine:** Let users reconnect to presence after a stranger takes their session id ([f4168e6](https://github.com/LevelFourAB/syncplane/commit/f4168e628b885a37d3f8de42531fb9d8ac636f77))
* **engine:** Make server queries match local queries for out-of-range integer literals ([c4f230b](https://github.com/LevelFourAB/syncplane/commit/c4f230bdf2fd4a1f221d3873f1b4bb6e206abf94))
* **engine:** Purge history rows on Postgres for any non-ASCII key prefix ([afbee60](https://github.com/LevelFourAB/syncplane/commit/afbee6064670ed3748e9b9857ce06f34160d38a0))
* **engine:** Re-check entity write rules under the write lock ([09d5789](https://github.com/LevelFourAB/syncplane/commit/09d578914916a213148342219c711c07b58fb6c8))
* **engine:** Refuse client ids longer than 128 bytes instead of failing the push on Postgres ([09013b3](https://github.com/LevelFourAB/syncplane/commit/09013b3d77d25f5f89e5843456c82792fbd84d0a))
* **engine:** Reject writes whose indexed value is too long instead of dead-lettering them on Postgres ([c364310](https://github.com/LevelFourAB/syncplane/commit/c364310a3d561335cfa0cca7fde32fc400847ad0))
* **engine:** Remove rows from clients when they stop following a reference ([1621ebc](https://github.com/LevelFourAB/syncplane/commit/1621ebc740621d32dd86cfeea523738f08ce9dc2))
* **engine:** Stop a node from overwriting a newer app schema ([e870857](https://github.com/LevelFourAB/syncplane/commit/e8708579bc32e3713990be9dc418ffee307f1335))
* **engine:** Stop an edited sync cursor from reading rows the read filter hides ([f1be064](https://github.com/LevelFourAB/syncplane/commit/f1be06456889479ffb25b504459028391d28f153))
* **engine:** Stop an older presence update from replacing a newer one ([fd8a4e2](https://github.com/LevelFourAB/syncplane/commit/fd8a4e28e7712d40f7ca011d8de85bc2974f0dd7))
* **engine:** Stop leased background work from running twice on one Postgres node ([d43855d](https://github.com/LevelFourAB/syncplane/commit/d43855d80f7c45ddd31b247f03b08f6ae4db5198))
* **engine:** Stop open sync streams from sending rows that a permission change hides ([dfeb228](https://github.com/LevelFourAB/syncplane/commit/dfeb228cbed6abd660dc3cd15afd4d0525e873c2))
* **engine:** Stop other users from blocking a client's writes by reusing its client ID ([af4c330](https://github.com/LevelFourAB/syncplane/commit/af4c330e65e6a6c392f11828b2ab6054ea8f059e))
* **engine:** Stop pulls failing after a windowed subscription is removed ([ba0e035](https://github.com/LevelFourAB/syncplane/commit/ba0e0353b1ea5acc1aaf67f895fc783b4315047d))
* **engine:** Use the default heartbeat for a value that is not positive ([24c7eee](https://github.com/LevelFourAB/syncplane/commit/24c7eee93a919c7165bdb1ab0e1b878f217d3dc7))
* Prevent dynamic registrations disagreeing after a server restart ([a4f7268](https://github.com/LevelFourAB/syncplane/commit/a4f726899471fc880886f90b9aca028974e8aaea))
* Return the matching rows for prefix searches on descending indexes ([d6bb403](https://github.com/LevelFourAB/syncplane/commit/d6bb4036dff425533fad09548d53408a188dca9d))
* Stop an edit to an empty-string key from replacing the whole map, set or list ([4fb0588](https://github.com/LevelFourAB/syncplane/commit/4fb0588dd94d8d5a5029240e0978cf1b1038d66a))
* Stop index reads from rounding integer literals on double fields ([056fecd](https://github.com/LevelFourAB/syncplane/commit/056fecd26c8c7a636b02c8ccd339e01940ffc3cf))
* Stop one malformed text edit from making a field unreadable for everyone ([80eb325](https://github.com/LevelFourAB/syncplane/commit/80eb32579f768274e2d093c907527c0116b0955e))
* Stop one write from making a collaborative text field impossible to edit ([4163afe](https://github.com/LevelFourAB/syncplane/commit/4163afebfb3a5c39ecee7db6146068b8b0878a4d))

## [0.1.0](https://github.com/LevelFourAB/syncplane/compare/v0.0.1...v0.1.0) (2026-09-25)


### Features

* Ability for an entity to reserve DELETE for trusted writes ([a85a506](https://github.com/LevelFourAB/syncplane/commit/a85a50610cd6dbc34b58c2e8985871d399489708))
* Add full-text search with relevance ranking ([28bff8f](https://github.com/LevelFourAB/syncplane/commit/28bff8f218564a02a14081271bf3ffe481f0d831))
* Add one-hop query support for relations ([5032134](https://github.com/LevelFourAB/syncplane/commit/503213431763dcb2f277292829c38e7cf4615a8b))
* Allow collaborative text fields inside keyed-list elements and map values ([5a25573](https://github.com/LevelFourAB/syncplane/commit/5a255734ea9f82ed4024fb9d08cab69fc494be0a))
* Allow grouping a match set by a path, with counts or distinct values ([53192c7](https://github.com/LevelFourAB/syncplane/commit/53192c782072c0461f215a2bea7079507d846ffc))
* Allow limiting which fields a client can write ([223b97a](https://github.com/LevelFourAB/syncplane/commit/223b97ad0f0f6e302f8324e1c652b1bb5fa73158))
* Allow oneof use for references ([16ca1db](https://github.com/LevelFourAB/syncplane/commit/16ca1dbfd56924754fc9850db0ba5a432ec53708))
* Allow syncing only certain fields ([3c2f02f](https://github.com/LevelFourAB/syncplane/commit/3c2f02f7c42c8d67651e6fc30585a419849f4ebe))
* **client:** Add devtools for easier debugging ([4c0468b](https://github.com/LevelFourAB/syncplane/commit/4c0468bf5689a4d8fce25771f58960ac2713891e))
* **client:** Add purgeLocalData for local sign-out ([d6cd206](https://github.com/LevelFourAB/syncplane/commit/d6cd2067e8e72ed60fa07691fe7a91d870016e5e))
* **client:** Add storagePrefix option ([0b2b9fd](https://github.com/LevelFourAB/syncplane/commit/0b2b9fde36f3eb76c9c4891efa108ab10b99e36a))
* **client:** Add syncplane/testing for easier testing ([e8df539](https://github.com/LevelFourAB/syncplane/commit/e8df539c099172fb72d8934ddaa3c0794b9fe254))
* **client:** Add useQueryAll hook for React ([7a589e4](https://github.com/LevelFourAB/syncplane/commit/7a589e439127e62343193fd264dadac91a98d973))
* **client:** Batch include hydration and add per-relation options ([b539af2](https://github.com/LevelFourAB/syncplane/commit/b539af2fdda26e118b37eea5f833e2837e91ea60))
* **client:** Cache movable lists ([19f3f12](https://github.com/LevelFourAB/syncplane/commit/19f3f1290b7251c996b86b59f11a8698383a8e0d))
* **client:** Coalesce a burst of local writes into one push ([1f15878](https://github.com/LevelFourAB/syncplane/commit/1f15878fe5cd7296372d1efaf0093b83d6bc42db))
* **client:** Cut local index storage by a third and speed up entity scans ([818de3d](https://github.com/LevelFourAB/syncplane/commit/818de3d80fbd05ef282ada64f39f457d3fe57c95))
* **client:** Examine large semi-join membership moves in probe-sized batches ([f7bcb5e](https://github.com/LevelFourAB/syncplane/commit/f7bcb5e9fd2ef0427d74bf9c5e1dc7f066b990c6))
* **client:** Fast path for search postings ([5bf4da2](https://github.com/LevelFourAB/syncplane/commit/5bf4da240618e49962f37ff9bc32ed8022d7a1a2))
* **client:** Hydrate include trees with one read per relation instead of one per row ([14d2d8b](https://github.com/LevelFourAB/syncplane/commit/14d2d8bc1e94cf3f5123c2e5eff012f49e153889))
* **client:** Improve bootstrap performance by overlapping decoding with store writes ([a84ca1e](https://github.com/LevelFourAB/syncplane/commit/a84ca1e2e7b638f0b2554720d071025239b6cfce))
* **client:** Improve query and indexing speeds ([cdd2cac](https://github.com/LevelFourAB/syncplane/commit/cdd2cac4fe577521058c50e5be73a8a88cd3a3a0))
* **client:** Improve querying performance with focus on total counts ([b6fd692](https://github.com/LevelFourAB/syncplane/commit/b6fd692d6d9469a339478c459be0bd8ccbd89bc6))
* **client:** Local queries report which path answered them through onSyncEvent ([e2d0e52](https://github.com/LevelFourAB/syncplane/commit/e2d0e529ae81d17bfa248ffd236df16db46e4bff))
* **client:** Push the changes that happened mid-push before pulling ([4e0ff7c](https://github.com/LevelFourAB/syncplane/commit/4e0ff7cbaf3f365b9328119f39e0800ab1bb363e))
* **client:** Rank a byWorkspace query by relevance ([9d2dd81](https://github.com/LevelFourAB/syncplane/commit/9d2dd814b9066bad7f000f7594b99a2e9cf4bc54))
* **client:** Read rows across workspaces by ref, and add useRowsAll ([47db376](https://github.com/LevelFourAB/syncplane/commit/47db3762c9dcccd07f35f2aef719155702699ee5))
* **client:** Read rows by key without deriving the entity, and add useRows ([a8e450c](https://github.com/LevelFourAB/syncplane/commit/a8e450c74ef4f22ecb50e5c5d5064276a653d911))
* **client:** Scope cross-workspace reads to named workspaces ([d276525](https://github.com/LevelFourAB/syncplane/commit/d27652578c0aedd4737e6a2cc9229db6cbea2865))
* **client:** Serve cross-workspace queries from secondary indexes ([b96cb5f](https://github.com/LevelFourAB/syncplane/commit/b96cb5f32b979114495961b8e896b9ef836eb957))
* **client:** Serve indexed queries as soon as a cold bootstrap completes ([92d346a](https://github.com/LevelFourAB/syncplane/commit/92d346a35ed1fab2282a0b0d7b459846cefd6fdc))
* **client:** Speed up watcher scope prune ([bd1f16c](https://github.com/LevelFourAB/syncplane/commit/bd1f16cf2cddeb3f6481d03de5564dcd50ee2dba))
* **client:** Support for scoped undo management ([e11e7f2](https://github.com/LevelFourAB/syncplane/commit/e11e7f21753ff700cf4d3f6535dd951896a27b9f))
* **client:** Support movable window and totals for live query results ([8a35d42](https://github.com/LevelFourAB/syncplane/commit/8a35d424d7b38ebbdf22eda53b772787278e7a44))
* Cross-namespace referencing of objects ([3b5f9eb](https://github.com/LevelFourAB/syncplane/commit/3b5f9eb15968ba9bd081bc6923b43ed07b1c807b))
* **engine:** Add MaxResyncScan to bound delta-resync work per Pull ([e05bda2](https://github.com/LevelFourAB/syncplane/commit/e05bda2b032977b4b4ecac8bdbc603ce95899766))
* **engine:** Add work queries so maintenance passes skip idle workspaces ([4fbdf46](https://github.com/LevelFourAB/syncplane/commit/4fbdf46ac73342ef2e02649c28276d88ca01e676))
* **engine:** Batch queued pokes into one NOTIFY round trip ([fccfdb6](https://github.com/LevelFourAB/syncplane/commit/fccfdb65a13917c93e9dfbcb7e55f4479605e6bc))
* **engine:** Dedup background work across nodes with per-workspace leases ([8e2f6a4](https://github.com/LevelFourAB/syncplane/commit/8e2f6a45771dbac4d6a5d118249be81e7255b9c7))
* **engine:** Enumerate index candidates by key without reading the rows twice ([56a6e7b](https://github.com/LevelFourAB/syncplane/commit/56a6e7ba3ab201f1bedc984f3ded0832d31579ae))
* **engine:** Hash-partition support for pgstore ([41f1593](https://github.com/LevelFourAB/syncplane/commit/41f1593d895868b11881352de977197af1d5b06c))
* **engine:** Improve background index build performance ([421c234](https://github.com/LevelFourAB/syncplane/commit/421c234620ecb421fc18ee38dfe4d6bdce371ef8))
* **engine:** Judge pgstore compatibility by what migrations did not version ([e48addd](https://github.com/LevelFourAB/syncplane/commit/e48addd2135c1e90e1344762bfe5f03ee1024dae))
* **engine:** Let a deployment turn off SQLite's memory-mapped reads ([3916fc5](https://github.com/LevelFourAB/syncplane/commit/3916fc54868593696c0f3098b479e9a2dd27e7bf))
* **engine:** Let text compaction run as its own sweep instead of as part of GC ([e84ca8d](https://github.com/LevelFourAB/syncplane/commit/e84ca8d957e980c3bd8872d784d16186f6f4b43e))
* **engine:** Load data in batches when querying via index ([d417b29](https://github.com/LevelFourAB/syncplane/commit/d417b29b0a1af0d420c88f7d96758bbbc63c3fec))
* **engine:** Maintain a forward follow closure from what changed instead of rebuilding it ([8667aea](https://github.com/LevelFourAB/syncplane/commit/8667aea2726e185cb154eb0b3196bee63c155632))
* **engine:** Prepare SQLite statements once per pool and reuse them across transactions ([35cc870](https://github.com/LevelFourAB/syncplane/commit/35cc870fb632dc27b04cd766fa7bc2000314a743))
* **engine:** Read a row's recorded actor and version through GetMeta ([337b588](https://github.com/LevelFourAB/syncplane/commit/337b58818559723c78bb499b95a7574495527c8b))
* **engine:** Refuse text inside repeated/map elements and list identities inside a oneof ([f3f5c2f](https://github.com/LevelFourAB/syncplane/commit/f3f5c2ffd3ad3328bc1b1df4fe602ba3c523bb4b))
* **engine:** Serve delta resync from indexes instead of scanning the workspace ([cd4a4dd](https://github.com/LevelFourAB/syncplane/commit/cd4a4dd340730e5cea87a0ce5cadaf98eeb63ab2))
* **engine:** Serve partial bootstraps from indexes, closures and windows ([bb8496b](https://github.com/LevelFourAB/syncplane/commit/bb8496b8a128a14faca4db8d8b48266a76c089df))
* **engine:** Speed up following subscriptions ([e2475d6](https://github.com/LevelFourAB/syncplane/commit/e2475d65966ff9ff65e63258e66744410c9c2096))
* **engine:** Subscribe to a workspace set through one wake channel instead of a goroutine per scope ([9c785e9](https://github.com/LevelFourAB/syncplane/commit/9c785e93767387a2407979baecf6c17386f4576a))
* **engine:** Wake the listener for a new channel without dropping its session ([c342eca](https://github.com/LevelFourAB/syncplane/commit/c342eca2b841ab43e96ebac53834fa27920f25c3))
* **engine:** Write only the index entries a row write changes ([37a782b](https://github.com/LevelFourAB/syncplane/commit/37a782b55a1babf9b65e60d23e4258174229c282))
* Entities can now declare who may create and update their rows ([aa1e2fa](https://github.com/LevelFourAB/syncplane/commit/aa1e2faf4e3890ea8d1dc962a99a56619f755753))
* Faceting support for queries ([1703088](https://github.com/LevelFourAB/syncplane/commit/170308850ebe410bc4447345645187a204edaeba))
* Feed cursors now name the store that issued them for safety ([88a4474](https://github.com/LevelFourAB/syncplane/commit/88a44745bd7cc3cf7daadb700c02910e08da61a1))
* Improve performance of pull via incremental window and closure membership ([92ee35d](https://github.com/LevelFourAB/syncplane/commit/92ee35d8725d57ca422de41a1725527e57e7dd1a))
* Improve querying and indexing for maps ([eee9ba7](https://github.com/LevelFourAB/syncplane/commit/eee9ba78e83657e25aea2e50a0180b074a1d5d0e))
* Initial version ([04e6d1e](https://github.com/LevelFourAB/syncplane/commit/04e6d1e10a60dfda94c24752dc2579925d42c394))
* Introduce presence via the concept of rooms ([97a7521](https://github.com/LevelFourAB/syncplane/commit/97a75214b768a60725d2abb072f4b50d4265e868))
* Limit workspace names to ASCII alphanumerics and a few control characters ([9c26b21](https://github.com/LevelFourAB/syncplane/commit/9c26b21719e4cc520b6d81e13ff56739bda37dfe))
* Make text compaction linear in document size ([6401917](https://github.com/LevelFourAB/syncplane/commit/64019174a758734a9b456207eeb69a22934d292e))
* Merge repeated workspace-qualified references per element, ([74d4a96](https://github.com/LevelFourAB/syncplane/commit/74d4a967ce03e7274e0895e8440a85f018726912))
* Persist cursor membership to avoid large cursors on the client ([3bd3e34](https://github.com/LevelFourAB/syncplane/commit/3bd3e343c3842f9a5b0cc3fc8f53895cae991730))
* Pick the narrowest ordered index for queries, not the first declared ([0365802](https://github.com/LevelFourAB/syncplane/commit/03658024b8c4f57848fbb84cbc305d486a40ac95))
* Read the first rows of each group instead of every row ([cd02056](https://github.com/LevelFourAB/syncplane/commit/cd0205615b2aef24ecd3735c01e9dbc094d82a98))
* Reduce cost of seeking with cursors ([67622d0](https://github.com/LevelFourAB/syncplane/commit/67622d0c530ef653f88fb40718fce3e50a617e44))
* Serve or filters, BETWEEN ranges and presence checks from indexes ([5be1341](https://github.com/LevelFourAB/syncplane/commit/5be1341bd3079fdb248239903ecadf6cd585e301))
* **server:** Clients no longer receive tombstones for hidden rows they never held ([262fcf4](https://github.com/LevelFourAB/syncplane/commit/262fcf49b1c94efe1de7dcb5c709fb41d7cd6bd9))
* Stable reverse relation names and ability to give them specific names ([dafe976](https://github.com/LevelFourAB/syncplane/commit/dafe9767533c69a193af94d4c6f37ceb6432ffc3))
* Store write attribution as subject/system ([6cd0587](https://github.com/LevelFourAB/syncplane/commit/6cd058729474bd44402a8ae009c79a85780fb918))
* Support for before/after cursors for queries ([907e53c](https://github.com/LevelFourAB/syncplane/commit/907e53c3df62c1067ddc0789b79ffb90c6d5f9d8))
* Support for storing history of entities ([77c7355](https://github.com/LevelFourAB/syncplane/commit/77c735545ddfa86a2bf1a56118cd64c7946a9444))
* Support serving data from read-replicas when using Postgres ([4919952](https://github.com/LevelFourAB/syncplane/commit/4919952a53434a57d3c0ec791f36551d634a61b4))
* Support syncing a row on-demand via holding it ([70b7f1a](https://github.com/LevelFourAB/syncplane/commit/70b7f1af3124375dd3396aedaee29df874cf557b))
* Warn when trying patch a row that doesn't exist ([fc2deb9](https://github.com/LevelFourAB/syncplane/commit/fc2deb9e10587817b6f8aa9c8ce6060e8814c965))


### Bug Fixes

* A live subscription no longer restates the client's whole membership on every change ([8317047](https://github.com/LevelFourAB/syncplane/commit/83170476bc06ccbed6b578b952d4eda863aa0b0c))
* A rejected edit no longer vanishes silently when a push response is lost ([b2f2d62](https://github.com/LevelFourAB/syncplane/commit/b2f2d6212687967bdb82dbb02e7d7815b5c0db24))
* **client/react:** Make useRow's compare-and-swap opt-in ([523d9a9](https://github.com/LevelFourAB/syncplane/commit/523d9a9cbe4a65667103fd6bc9062498b7668a42))
* **client:** A dead-letter that fails mid-way no longer wedges the queue ([e6a7af6](https://github.com/LevelFourAB/syncplane/commit/e6a7af63a39ecbdd2d1602e9a40249a9756b5f45))
* **client:** A mutation the server fails no longer wedges the queue forever ([475ef59](https://github.com/LevelFourAB/syncplane/commit/475ef596906efb06e3df2f6e0c3f4e87b82f2033))
* **client:** Adding a workspace mid-session no longer drops its queued edits ([0f0f1d5](https://github.com/LevelFourAB/syncplane/commit/0f0f1d5e2fabac19c1beb2b46ad4ebccc2aef1d5))
* **client:** An unapplicable queued mutation no longer wedges a workspace ([d4edfd5](https://github.com/LevelFourAB/syncplane/commit/d4edfd530acac2899e9247f7000700fa6ff2b040))
* **client:** Close mutation-id gaps that can wedge a workspace queue ([8dd7555](https://github.com/LevelFourAB/syncplane/commit/8dd75553eda28585c3a0082788587299e1c9ce29))
* **client:** Edits made after a store format upgrade no longer go missing ([4dcc222](https://github.com/LevelFourAB/syncplane/commit/4dcc222341ad14608e3838e408f5343b5ba4a1ea))
* **client:** Fold pending mutations that share a CAS guard ([2548b97](https://github.com/LevelFourAB/syncplane/commit/2548b97c17e347e8d78a00e6bd6bab1689f14357))
* **client:** Other tabs no longer lose edits after a store format upgrade ([a803748](https://github.com/LevelFourAB/syncplane/commit/a80374853b079340208e069a345c0c4e90b7db12))
* **client:** Re-adding a purged workspace no longer breaks future editing ([fd28944](https://github.com/LevelFourAB/syncplane/commit/fd289443d71c6f6901763706148b32c0dcbd21f4))
* Clients no longer re-bootstrap per text edits to an undecodable entity ([474c80f](https://github.com/LevelFourAB/syncplane/commit/474c80f2058b15d397cefcc519beb1a457d379a9))
* **client:** Spread reconnects across the whole backoff window, and pace the presence stream ([9b816c7](https://github.com/LevelFourAB/syncplane/commit/9b816c7a06ffee85b73707b20b9029077e4b3b4d))
* **client:** Stop a compaction from dropping typed text and re-sending a barrier mutation ([7faf1a6](https://github.com/LevelFourAB/syncplane/commit/7faf1a6819b1afab4cead48c325939a62a3f841a))
* **client:** Stop online edits from stalling behind full-scope watch rescans ([7c81015](https://github.com/LevelFourAB/syncplane/commit/7c81015d80e355b65325df8985c0abd331a53208))
* **client:** Text editing no longer dies with a "different placement" merge conflict ([ab0d6f5](https://github.com/LevelFourAB/syncplane/commit/ab0d6f54b141788ef81fe7ce0d9bbf72c6af7a22))
* **client:** Text writer identities stay unique in browsers without Web Locks ([0e03508](https://github.com/LevelFourAB/syncplane/commit/0e035087468ed44e161ec5f6414e05f64e475a06))
* **client:** Undoing a delete now conflicts instead of overwriting a row that took the key ([64ae63b](https://github.com/LevelFourAB/syncplane/commit/64ae63bc229c57e8573a97d06049bce73ef6e485))
* **client:** Whole-value text writers now coalesce into word-sized undo groups ([9767f75](https://github.com/LevelFourAB/syncplane/commit/9767f75e30602e4db7229d448255e7f473486be4))
* **engine:** Stop a co-present user from evicting or overwriting another's presence ([a652cfb](https://github.com/LevelFourAB/syncplane/commit/a652cfb93bc194fa7cd356073f003aac1a4f11cb))
* **engine:** Stop a paged pull from skipping a tombstone GC removed at the cursor's version ([864a847](https://github.com/LevelFourAB/syncplane/commit/864a8476d5a00e969456183f740517a846ccbc5d))
* **engine:** Stop a pull from issuing three queries per changed row of a followed window ([0835479](https://github.com/LevelFourAB/syncplane/commit/08354797bef52dbe833acbaafbdc4fb77e2417f1))
* **engine:** Stop GC from letting an old push overwrite newer writes ([e0fa49b](https://github.com/LevelFourAB/syncplane/commit/e0fa49bf9df927996249ac45897fedd3eb920632))
* **engine:** Stop leaking authentication errors to clients ([7d879cc](https://github.com/LevelFourAB/syncplane/commit/7d879cc540b2dc351518a78e68028b4c88fbc46a))
* **engine:** Stop losing followed rows after changing subscriptions ([eca80c1](https://github.com/LevelFourAB/syncplane/commit/eca80c1bbf5f6b15fe75194e6d7da571acf18369))
* **engine:** Stop one-row change from costing a full referrer scan per included target on Pull ([d03698c](https://github.com/LevelFourAB/syncplane/commit/d03698c2e07c55304bc48f2542a8e4be34e6a429))
* **engine:** Stop the pull after a subscription change from shipping text deltas for rows the client never received ([b336cbb](https://github.com/LevelFourAB/syncplane/commit/b336cbb18135bafd76f07dbcc91d78319ddc73e4))
* **engine:** Stop windowed pulls on PostgreSQL failing after a change-log scan ([2994a56](https://github.com/LevelFourAB/syncplane/commit/2994a564047776ca7dd302d6daae8797f5d39313))


### Performance Improvements

* Ack a whole push batch with one watermark write per client ([6b68d44](https://github.com/LevelFourAB/syncplane/commit/6b68d44316f7a0ad87bff2e5184b17de4d41e335))
* **client:** Apply projected decoding and size-based index pricing to groupCount, distinct and groupRows ([44b90e8](https://github.com/LevelFourAB/syncplane/commit/44b90e8f407231f7195e71c3898c55274548cdee))
* **client:** Decode only the fields needed for queries, for example for pure filtering and factes ([67b1256](https://github.com/LevelFourAB/syncplane/commit/67b125640258855d63b7a236716d047957a1434c))
* **client:** Decode only the searchable fields when a match query falls back to a scan ([d76b0ba](https://github.com/LevelFourAB/syncplane/commit/d76b0bad940972a1f56daf07b301276840e0bfe7))
* **client:** Keep local queries fast when many writes are waiting to sync ([5c3151c](https://github.com/LevelFourAB/syncplane/commit/5c3151c899fe092c37c72d3bc9c1f33037bc2203))
* **client:** Memoize descriptor-derived reference and field-behavior lookups ([5ed51f0](https://github.com/LevelFourAB/syncplane/commit/5ed51f059c100d5838619d575404b1ae3b417221))
* **client:** Overlap IndexedDB reads with row decoding on full-entity scans ([eac49cb](https://github.com/LevelFourAB/syncplane/commit/eac49cb01b8bb64bc96a87258eace802edf1085b))
* **client:** Read index-served rows in bulk instead of one IndexedDB request per row ([3c409e4](https://github.com/LevelFourAB/syncplane/commit/3c409e4446d5c68bf5bb3b7fbcf44e07491a60cd))
* **client:** Skip the second IndexedDB round trip when a local edit notifies watchers ([f4896bb](https://github.com/LevelFourAB/syncplane/commit/f4896bb036a718f92fc45cff58521ff4f16ea7f6))
* **client:** Stop decoding every pending row whole on each indexed query ([07005f1](https://github.com/LevelFourAB/syncplane/commit/07005f16aa81d5e6c45feb66705a13ee6445422c))
* **client:** Store search positions in their own record so most row updates skip them ([619802f](https://github.com/LevelFourAB/syncplane/commit/619802f2ddcca8c685eea5e05a01a18286adc659))
* **engine:** Batch of mutations to PostgreSQL ([405da77](https://github.com/LevelFourAB/syncplane/commit/405da775bb81584e55181f526664389ef8985b41))
* **engine:** Make Postgres pushes faster by inserting only new index entries ([67210d2](https://github.com/LevelFourAB/syncplane/commit/67210d238a5433b7e00f8caeb0dd9e1f39a52882))
* **engine:** Reduce number of Postgres roundtrips needed to answer a catch-up pull ([7bb6962](https://github.com/LevelFourAB/syncplane/commit/7bb696210aed2d8efa7bd1e7593098cb716eca04))
* **engine:** Resolve followed reference targets in batches ([e06db1b](https://github.com/LevelFourAB/syncplane/commit/e06db1bb570cad43a1af2bc4462d07df43d7e030))
* Keep a live query over a large result fast by rebuilding only the rows a write moved ([ed73a53](https://github.com/LevelFourAB/syncplane/commit/ed73a5329577454de437a12bfe0460063e8a809e))
* Make local edits faster by reading a write's rows in one IndexedDB transaction ([af0951a](https://github.com/LevelFourAB/syncplane/commit/af0951a09d264636e931a676922d2a3e73049826))
* Serve faceted queries from the index instead of a full scan when beneficial ([c6fc5bd](https://github.com/LevelFourAB/syncplane/commit/c6fc5bd60c8142546ea61b512e6b14c09de6e56e))
