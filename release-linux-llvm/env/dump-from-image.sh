#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
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
#
# Dump a ready-to-use TiFlash build environment out of an *existing*
# tiflash-llvm-base image, in the same layout as the tarball produced by
# `prepare-sysroot.sh` (see ../README.md):
#
#   tiflash-env/
#   |-- loader                (copied from this directory, unmodified)
#   |-- loader-env-dump
#   |-- prepare-sysroot.sh
#   |-- tiflash-linker
#   `-- sysroot/              clang, lld, comp*rt, libc++, cmake, ccache, openssl
#
# It only copies files out of the image -- it does NOT rebuild LLVM, which is what
# makes it take seconds instead of hours.  Usage (normally via
# `make dump-tiflash-env`):
#
#   ./dump-from-image.sh <image> <output-dir> [<arch>]
#
# The image and prepare-sysroot.sh lay things out differently even though both end
# up under a single `sysroot` prefix, so two fixups are needed:
#
#   * OpenSSL: the image installs it under /usr/local/opt/openssl (see
#     misc/install_openssl.sh, `--prefix=/usr/local/opt/openssl`), while
#     prepare-sysroot.sh installs it straight into the prefix.  `loader` sets
#     OPENSSL_ROOT_DIR=<sysroot>, so leaving it in opt/ would make
#     find_package(OpenSSL) fail.  Move it up to the prefix root, which makes the
#     dumped tree layout-identical to the prepare-sysroot.sh one (that also brings
#     back sysroot/bin/openssl and sysroot/lib/pkgconfig/openssl.pc).
#   * CMake: lives in /opt/cmake in the image, merge it into sysroot/ so that
#     sysroot/bin/cmake (what `loader` points CMAKE at) exists.
#
# Not included, to stay identical to the tarballs under release-linux-llvm/env:
# ninja (the old packages do not ship it either) and Rust (the image keeps it in
# /root/.rustup + /root/.cargo; users are expected to bring their own rustup).

set -euo pipefail

IMAGE=${1:?usage: $0 <image> <output-dir> [<arch>]}
OUT=${2:?usage: $0 <image> <output-dir> [<arch>]}
ARCH=${3:-$(uname -m)}
DOCKER=${DOCKER:-docker}

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
CT="tiflash-env-dump-$$"

cleanup() { "${DOCKER}" rm -f "${CT}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

rm -rf "${OUT}"
mkdir -p "${OUT}/sysroot"

echo ">>> dumping ${IMAGE} into ${OUT}"
"${DOCKER}" create --name "${CT}" "${IMAGE}" >/dev/null
"${DOCKER}" cp "${CT}:/usr/local/." "${OUT}/sysroot/"
"${DOCKER}" cp "${CT}:/opt/cmake/." "${OUT}/sysroot/"

echo ">>> moving openssl into the sysroot prefix root"
if [ -d "${OUT}/sysroot/opt/openssl" ]; then
    cp -a "${OUT}/sysroot/opt/openssl/." "${OUT}/sysroot/"
    rm -rf "${OUT}/sysroot/opt"
else
    echo "!!! warning: ${OUT}/sysroot/opt/openssl not found, skipping" >&2
fi

echo ">>> copying the loader scripts"
cp "${SCRIPTPATH}/loader" \
   "${SCRIPTPATH}/loader-env-dump" \
   "${SCRIPTPATH}/prepare-sysroot.sh" \
   "${SCRIPTPATH}/tiflash-linker" \
   "${OUT}/"
chmod +x "${OUT}/loader" "${OUT}/loader-env-dump" \
         "${OUT}/prepare-sysroot.sh" "${OUT}/tiflash-linker"

echo ">>> done. Use it with:"
echo "    cd ${OUT}"
echo "    ./loader-env-dump > ~/.tiflash_env && source ~/.tiflash_env"
