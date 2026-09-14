# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DOCKER ?= docker
ARCH=$(shell uname -m)
MOUNT=$(shell realpath tiflash-env)
SUFFIX ?= ""
# The base image holding the toolchain. Depending on the target it is used either as
# the build environment (prepare-sysroot.sh) or as the source to dump from.
BUILDER_IMAGE ?= hub.pingcap.net/tiflash/tiflash-llvm-base:rocky8$(SUFFIX)

# make the shell scripts as phony so that always replace with newer one
.PHONY: scripts clean dump-tiflash-env

tiflash-env:
	mkdir -p tiflash-env

scripts: tiflash-env
	cp prepare-sysroot.sh tiflash-env
	cp loader tiflash-env
	cp loader-env-dump tiflash-env
	cp tiflash-linker tiflash-env

tiflash-env-$(ARCH).tar.xz: scripts
	# Add the z to modify the selinux label of the host file or directory being mounted into the container.
	# Reference: https://docs.docker.com/storage/bind-mounts/#configure-the-selinux-label
	$(DOCKER) run --rm -v $(MOUNT):/tiflash-env:z $(BUILDER_IMAGE) /tiflash-env/prepare-sysroot.sh
	tar -cvaf tiflash-env-$(ARCH).tar.xz tiflash-env

# Dump a ready-to-use toolchain out of an EXISTING builder image, without rebuilding
# LLVM (seconds instead of hours). The result has the same layout as
# tiflash-env-$(ARCH).tar.xz above: a top-level tiflash-env/ holding the loader
# scripts plus sysroot/ -- see dump-from-image.sh for the details (openssl is moved
# from sysroot/opt/openssl up to the prefix root so the unmodified loader works).
# Note: it overwrites tiflash-env/ and tiflash-env-$(ARCH).tar.xz.
#
#   make dump-tiflash-env BUILDER_IMAGE=hub.pingcap.net/tiflash/tiflash-llvm-base:rocky8-llvm-23.1.1
#   make dump-tiflash-env BUILDER_IMAGE=ghcr.io/pingcap-qe/cd/builders/tiflash:v2025.4.15-rocky8-llvm-23.1.1
#
# Then, on the machine that will use it:
#   tar xf tiflash-env-$(ARCH).tar.xz && cd tiflash-env
#   ./loader-env-dump > ~/.tiflash_env && source ~/.tiflash_env
dump-tiflash-env:
	./dump-from-image.sh "$(BUILDER_IMAGE)" tiflash-env $(ARCH)
	tar -cvaf tiflash-env-$(ARCH).tar.xz tiflash-env

clean:
	rm -rf tiflash-env tiflash-env-$(ARCH).tar.xz
