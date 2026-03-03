# CHANGELOG

<!-- version list -->

## v1.9.2 (2026-03-03)

### Bug Fixes

- Load env for secret
  ([`2fcf159`](https://github.com/celine-eu/celine-utils/commit/2fcf1590ef6b1d3c6da49a1887dd77285226d2f8))


## v1.9.1 (2026-03-02)

### Bug Fixes

- Update prefect version to 3.6.18
  ([`7a908b6`](https://github.com/celine-eu/celine-utils/commit/7a908b6078c8d9fa3bd68892f1dc6e818291738b))


## v1.9.0 (2026-02-28)

### Bug Fixes

- Add verify_ssl
  ([`c646c93`](https://github.com/celine-eu/celine-utils/commit/c646c936e0021fc59fa30e88a1c1efa82739ac26))

### Features

- Upgrade sdk
  ([`f136b7b`](https://github.com/celine-eu/celine-utils/commit/f136b7bac68b267cc4a4aad42efe2ab7e8192472))


## v1.8.5 (2026-02-26)

### Bug Fixes

- Revert threadpool executor
  ([`2f78c4e`](https://github.com/celine-eu/celine-utils/commit/2f78c4e63a247e52f8913df7621b4fae2ce72fca))


## v1.8.4 (2026-02-26)

### Bug Fixes

- Improve dbt lineage logging
  ([`74c5c18`](https://github.com/celine-eu/celine-utils/commit/74c5c181d9a2a5da19a4cdf3d0dd2be3ad9cfea8))


## v1.8.3 (2026-02-25)

### Bug Fixes

- Sync callbacks
  ([`69d6408`](https://github.com/celine-eu/celine-utils/commit/69d640808a82cbb580dd8f89f1d0f962d4f79328))


## v1.8.2 (2026-02-25)

### Bug Fixes

- Handle mqtt failure on connect
  ([`a2ef16a`](https://github.com/celine-eu/celine-utils/commit/a2ef16a9da564b8ecb339334b342320f489c8ae5))


## v1.8.1 (2026-02-25)

### Bug Fixes

- Rebuild
  ([`3ec1da3`](https://github.com/celine-eu/celine-utils/commit/3ec1da352fe6e7354ff1449b0a3b8e9eec418fe0))


## v1.8.0 (2026-02-25)

### Features

- Upgrade flow hooks api
  ([`6bf7740`](https://github.com/celine-eu/celine-utils/commit/6bf77404a5d9e93e6094f1c8e454dce6bb51471f))


## v1.7.1 (2026-02-25)

### Bug Fixes

- Do not raise if connect fails, keep pipeline running
  ([`720716b`](https://github.com/celine-eu/celine-utils/commit/720716bde64b2d749d2af9bd523b9b008b875816))

### Chores

- **deps**: Bump openlineage-python from 1.39.0 to 1.41.0
  ([`7cfd741`](https://github.com/celine-eu/celine-utils/commit/7cfd741fb3504a6e437fc76c02eed62a9b53d62b))

- **deps**: Bump pydantic-settings from 2.11.0 to 2.12.0
  ([`ed18c86`](https://github.com/celine-eu/celine-utils/commit/ed18c868b51d285fbfe02b06ff3d7d04e4c4dffb))

- **deps**: Bump python-dotenv from 1.1.1 to 1.2.1
  ([`7cedf82`](https://github.com/celine-eu/celine-utils/commit/7cedf821d38f6b2d75096751d3cafeaafb8fc82c))

- **deps**: Bump the runtime-dependencies group across 1 directory with 3 updates
  ([`6895028`](https://github.com/celine-eu/celine-utils/commit/68950280109be125d278671bb4e00a9da31df489))

- **deps-dev**: Bump the development-dependencies group across 1 directory with 2 updates
  ([`a8e2d60`](https://github.com/celine-eu/celine-utils/commit/a8e2d606fe92105645c792a826ddeb777832841e))

### Continuous Integration

- Bump actions/checkout from 6.0.1 to 6.0.2 in the actions group
  ([`6c06e1e`](https://github.com/celine-eu/celine-utils/commit/6c06e1ed5c732de19620f655e18ce18aebd12c90))


## v1.7.0 (2026-02-23)

### Chores

- Upgrade celine-sdk
  ([`4a25a1b`](https://github.com/celine-eu/celine-utils/commit/4a25a1bfeb8056b2f59119e12222df87b576f6c7))

- Upgrade taskfile with setup
  ([`68db655`](https://github.com/celine-eu/celine-utils/commit/68db655eeae55a4894cd971567325b0e94b34345))

### Features

- Upgrade sdk
  ([`50e30de`](https://github.com/celine-eu/celine-utils/commit/50e30def83bc575928a1aa385af5ccfa1c914e52))


## v1.6.3 (2026-02-20)

### Bug Fixes

- Emit flow name
  ([`2e3fead`](https://github.com/celine-eu/celine-utils/commit/2e3feadf8e52efa62aad4ce0173bfaabf20ae8e2))


## v1.6.2 (2026-02-19)

### Bug Fixes

- Use shared event for pipelines run
  ([`dd6dc51`](https://github.com/celine-eu/celine-utils/commit/dd6dc517b472fc532508fa8c4da0f3e7136cff79))

### Chores

- Udpate celine sdk
  ([`019a22c`](https://github.com/celine-eu/celine-utils/commit/019a22cf1b857d1e4109b0beb04c59e6705e8ce0))


## v1.6.1 (2026-02-19)

### Bug Fixes

- Use async mqtt connect
  ([`f435a3d`](https://github.com/celine-eu/celine-utils/commit/f435a3da6cf396579f81c2fdce79a48c1568ca32))


## v1.6.0 (2026-02-19)

### Bug Fixes

- Proper load env
  ([`d38cfe6`](https://github.com/celine-eu/celine-utils/commit/d38cfe6a38c15145a4ca6d3572c5b0cfce4deb0a))

### Features

- Refactor pipeline api, introduce context manager for pipeline tracking
  ([`f669d1d`](https://github.com/celine-eu/celine-utils/commit/f669d1d78303e366591c44cd304877f1498bbb7e))


## v1.5.0 (2026-02-18)

### Bug Fixes

- Correct topic
  ([`3b2dcbc`](https://github.com/celine-eu/celine-utils/commit/3b2dcbcd7486474cc2c107a2b938e93454c104da))

- Expose lineage env, properly load sdk settings
  ([`aaae068`](https://github.com/celine-eu/celine-utils/commit/aaae0689a5faf9896150c62c7cb3f97f8d5265d3))

### Chores

- Add dumpster
  ([`5236430`](https://github.com/celine-eu/celine-utils/commit/5236430f07a5bfc7822fe8c0d0388f0a0124336e))

- Move to src
  ([`8da170a`](https://github.com/celine-eu/celine-utils/commit/8da170a6b45a68c5e87156581161b8317e4fa60f))

- Run actions on selected tags/branches
  ([`f15c1d4`](https://github.com/celine-eu/celine-utils/commit/f15c1d42f8c75aaed1cc109486043b26104d2034))

- Up debugger config
  ([`1a8427a`](https://github.com/celine-eu/celine-utils/commit/1a8427a15e632b680ce274ba152217c3f82563f2))

### Features

- Add user column filter
  ([`e46c960`](https://github.com/celine-eu/celine-utils/commit/e46c9608645cfbd57fe90eb317da98d69a12c456))

- Expand envs from pipeline config
  ([`c38e892`](https://github.com/celine-eu/celine-utils/commit/c38e8928c22807183bfbd40accc5b06d5c64d5db))


## v1.4.4 (2025-12-28)

### Bug Fixes

- Missing dbt facets fields
  ([`be4cd67`](https://github.com/celine-eu/celine-utils/commit/be4cd671f23b5bb5f9fb0f1cf31bbdb88a69d975))


## v1.4.3 (2025-12-28)

### Bug Fixes

- Add governance title, description fields
  ([`8cc94c4`](https://github.com/celine-eu/celine-utils/commit/8cc94c4b490dc30cac892614c51ac83dd5a0291c))

### Chores

- Typo
  ([`e4e4df8`](https://github.com/celine-eu/celine-utils/commit/e4e4df89cebf741412bdd7d7bf9d0f9496a115ae))


## v1.4.2 (2025-12-18)

### Bug Fixes

- Correct return types
  ([`2e390c1`](https://github.com/celine-eu/celine-utils/commit/2e390c16f668b384e85ad74e040d197a713e6a81))

### Chores

- Update ref
  ([`924a161`](https://github.com/celine-eu/celine-utils/commit/924a1617e2923499627d85981e842949c45f5a17))


## v1.4.1 (2025-12-17)

### Bug Fixes

- Include direct pipelines deps
  ([`6788637`](https://github.com/celine-eu/celine-utils/commit/678863718a7e214d9220480ae0286bc0f2a9b93a))


## v1.4.0 (2025-12-17)

### Features

- Add auth to marquez call
  ([`3d818d6`](https://github.com/celine-eu/celine-utils/commit/3d818d6ef8be9e98aae644f0aa603e788160f989))

- Add governance fields, update docs
  ([`047acd8`](https://github.com/celine-eu/celine-utils/commit/047acd8ef6d24f95a0e6c86d9a08cd0916b04525))

- Refactor base module ns
  ([`1c51c33`](https://github.com/celine-eu/celine-utils/commit/1c51c33cb15131f1aa4c9e6aaea26fad45afc80e))


## v1.3.0 (2025-12-15)

### Bug Fixes

- Review and fix tests
  ([`342e8b1`](https://github.com/celine-eu/celine-utils/commit/342e8b1f10e89b9e071c8df02bf4f5e26108c9ff))

- Skip lineage lookup if disable. update docs
  ([`3a23f1b`](https://github.com/celine-eu/celine-utils/commit/3a23f1bdc4d1636a1ce02bc343959ab073b2cbce))

- Update schema uris
  ([`85c6a4a`](https://github.com/celine-eu/celine-utils/commit/85c6a4a59a02bbaea84626e298c73253e9f428b5))

### Chores

- Add update docs
  ([`ed1b5e8`](https://github.com/celine-eu/celine-utils/commit/ed1b5e8d788a6415536012bf01c3ee8b109ea2bf))

- Dump source respecting .gitignore
  ([`6b1acf9`](https://github.com/celine-eu/celine-utils/commit/6b1acf9701e277ea8d438632038e0be05a675d93))

- Fix cmd ref
  ([`8e3967a`](https://github.com/celine-eu/celine-utils/commit/8e3967a57547b5ad9de00483786fae71c5ddbc7b))

- Update readme
  ([`4ec9ea7`](https://github.com/celine-eu/celine-utils/commit/4ec9ea7aa8a2328f202dda43e208f1605fc7e256))

- **deps**: Bump the runtime-dependencies group across 1 directory with 3 updates
  ([`a45ea5d`](https://github.com/celine-eu/celine-utils/commit/a45ea5d3bf8fd656f99b7da212d6927b92697f2d))

- **deps-dev**: Bump the development-dependencies group with 5 updates
  ([`fd48ba8`](https://github.com/celine-eu/celine-utils/commit/fd48ba8324961552bc159c27139393f0cb194e73))

### Continuous Integration

- Bump the actions group across 1 directory with 3 updates
  ([`85cf91a`](https://github.com/celine-eu/celine-utils/commit/85cf91a8bb2f6eb45b0c4e5bb77a427f65379dba))

### Documentation

- Add governance
  ([`da543f1`](https://github.com/celine-eu/celine-utils/commit/da543f1c820eed651c9502f6ca45515fa7fcf7b1))

- Add pipeline tutorial
  ([`ca989b0`](https://github.com/celine-eu/celine-utils/commit/ca989b081f99213126cf179672b589185a40c1fc))

### Features

- Allow to disable openlineage
  ([`24c5e99`](https://github.com/celine-eu/celine-utils/commit/24c5e999f6072bb3c7a0c30748c2690fcd34b19b))


## v1.2.0 (2025-12-10)

### Features

- Add celine cli create pipeline
  ([`cbc5e75`](https://github.com/celine-eu/celine-utils/commit/cbc5e7524ffa02f385147a07b11642fa546446d1))

- Add cli generae tests
  ([`7db3f84`](https://github.com/celine-eu/celine-utils/commit/7db3f84e581944a9c46aa35d9dddeadf86da6884))

- Add governance features
  ([`7f40e44`](https://github.com/celine-eu/celine-utils/commit/7f40e4407b55930cc10b82caa92ea46a63eba866))

- Improve logging, improve pipeline runner output
  ([`5827d02`](https://github.com/celine-eu/celine-utils/commit/5827d021abfd941ad5f4c98a9b8e28dcc26b88f2))

- Pipeline CLI
  ([`d6c82aa`](https://github.com/celine-eu/celine-utils/commit/d6c82aa0c38fbac99709677d8be08537e1e7065f))


## v1.1.1 (2025-11-29)

### Bug Fixes

- Expose function
  ([`70830fb`](https://github.com/celine-eu/celine-utils/commit/70830fb38810203e320f6523e4c0822ef3b96250))


## v1.1.0 (2025-11-29)

### Chores

- Add meltano fail test
  ([`33c36fd`](https://github.com/celine-eu/celine-utils/commit/33c36fde8b64e5a70676dbba57709fd7d3431857))

### Features

- Add run_dbt_operation.
  ([`8792fdb`](https://github.com/celine-eu/celine-utils/commit/8792fdbee3ba158b7a6ead2bcfad34b3e7c7855b))


## v1.0.9 (2025-11-22)

### Bug Fixes

- More logging
  ([`47b1484`](https://github.com/celine-eu/celine-utils/commit/47b148451b68472ec333c682abd0ec58d241ec3b))


## v1.0.7 (2025-10-17)

### Bug Fixes

- Include all subpackages
  ([`a505b00`](https://github.com/celine-eu/celine-utils/commit/a505b005a925962d9b73c073e4a451ba5fa22a57))


## v1.0.6 (2025-10-17)

### Bug Fixes

- Review release task
  ([`43fd4d8`](https://github.com/celine-eu/celine-utils/commit/43fd4d86a15316f68b9366900321f74b9d64f213))


## v1.0.5 (2025-10-17)

### Bug Fixes

- Review config
  ([`b7ae3fe`](https://github.com/celine-eu/celine-utils/commit/b7ae3fe44e48c505c077d19f6b14acf0c0544b41))


## v1.0.4 (2025-10-17)

### Bug Fixes

- Review config
  ([`a844164`](https://github.com/celine-eu/celine-utils/commit/a844164863bb9c1c3c0a15ec727fbbaa5231af33))


## v1.0.3 (2025-10-17)

### Bug Fixes

- Update config
  ([`1c80f57`](https://github.com/celine-eu/celine-utils/commit/1c80f578e076d08a7ef58a110c3863452ad29bcf))


## v1.0.2 (2025-10-17)

### Bug Fixes

- Add taskfile for release
  ([`c57dfaf`](https://github.com/celine-eu/celine-utils/commit/c57dfaff7fce83ed326e4f0ab1122d2a8a5ee74c))

### Chores

- **deps**: Bump bcrypt from 4.3.0 to 5.0.0
  ([`9263030`](https://github.com/celine-eu/celine-utils/commit/92630304ce803ab58520053b556cdf235cff7848))

- **deps**: Bump openlineage-python from 1.37.0 to 1.39.0
  ([`234a6b2`](https://github.com/celine-eu/celine-utils/commit/234a6b20ba40890d17881ba0fd5930763a89a890))

- **deps**: Bump pydantic from 2.11.7 to 2.12.2
  ([`b8a8fde`](https://github.com/celine-eu/celine-utils/commit/b8a8fde4baae123da9c6d12be8fcdda924a0778b))

- **deps**: Bump pydantic-settings from 2.10.1 to 2.11.0
  ([`079d045`](https://github.com/celine-eu/celine-utils/commit/079d045093b0b3e9bfc617ae1915bd623ea33e9d))

- **deps**: Bump the runtime-dependencies group with 7 updates
  ([`cfe2c60`](https://github.com/celine-eu/celine-utils/commit/cfe2c60a7b63c56e983ac3861b9e722022d2d458))

### Continuous Integration

- Bump hynek/build-and-inspect-python-package in the actions group
  ([`11352d0`](https://github.com/celine-eu/celine-utils/commit/11352d0c2ce9bb8fe2be499a3ff08e9e71e49b33))


## v1.0.1 (2025-10-17)

### Bug Fixes

- Update config
  ([`dcc6cee`](https://github.com/celine-eu/celine-utils/commit/dcc6ceece5e338c1192baa70c71628fa56b75a35))

### Chores

- **config**: Update semantic config
  ([`da4698d`](https://github.com/celine-eu/celine-utils/commit/da4698d2b2ff21a0b50f4a31da93c5360ff33dfc))


## v1.0.0 (2025-10-17)

- Initial Release
